#!/usr/bin/env python3
"""
=============================================================================
  Balfund HA + SMA(1) Strategy Trader  v1.0  [Terminal Edition]
=============================================================================
  Strategy  : Heikin Ashi candles + SMA(1) on HA Open
              GREEN HA (Close > Open) → BUY
              RED   HA (Close < Open) → SELL SHORT
              Always in market. Entry on startup from current HA color.

  Instruments: 21 NSE EQ stocks (INTRADAY)
               GOLDTEN     (MCX FUTCOM  · 1 lot = 10g   · qty=1 in API)
               SILVERMICRO (MCX FUTCOM  · 1 lot = 1000g · qty=1 in API)

  Run       : python ha_strategy_dhan.py
  Stop      : Ctrl + C
=============================================================================
"""
from __future__ import annotations

import csv
import io
import json
import os
import sys
import time
import threading
import requests
import websocket   # pip install websocket-client

# Parse Dhan WS binary packets (dhan_ws_client.py must be in same folder)
from dhan_ws_client import (
    REQ_SUB_TICKER,
    RESP_TICKER,
    parse_header_8,
    parse_ticker,
)
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─────────────────────────────────────────────────────────────────────────────
#  ANSI COLOURS (VSCode terminal supports these)
# ─────────────────────────────────────────────────────────────────────────────
GRN  = "\033[92m"
RED  = "\033[91m"
YEL  = "\033[93m"
BLU  = "\033[94m"
CYN  = "\033[96m"
GRY  = "\033[90m"
BOLD = "\033[1m"
RST  = "\033[0m"

def green(s):  return f"{GRN}{s}{RST}"
def red(s):    return f"{RED}{s}{RST}"
def yellow(s): return f"{YEL}{s}{RST}"
def blue(s):   return f"{BLU}{s}{RST}"
def cyan(s):   return f"{CYN}{s}{RST}"
def grey(s):   return f"{GRY}{s}{RST}"
def bold(s):   return f"{BOLD}{s}{RST}"

# ─────────────────────────────────────────────────────────────────────────────
#  DHAN API ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────
INTRADAY_URL          = "https://api.dhan.co/v2/charts/intraday"
ORDER_URL             = "https://api.dhan.co/v2/orders"
ORDER_STATUS_BASE_URL = "https://api.dhan.co/v2/orders"
INSTRUMENT_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"

MASTER_CSV_CACHE      = Path("dhan_master_cache.csv")
MASTER_CACHE_MAX_AGE_H = 24          # hours before re-download

# ─────────────────────────────────────────────────────────────────────────────
#  MCX LOT SIZES
#  Dhan MCX API takes quantity in LOTS (same way NSE takes shares).
#  quantity = 1 → 1 lot of GOLDTEN (10g) or SILVERMICRO (1000g)
# ─────────────────────────────────────────────────────────────────────────────
MCX_LOT_MULTIPLIERS: Dict[str, int] = {
    "GOLDTEN":     1,   # 1 lot = 10g
    "SILVERMICRO": 1,   # 1 lot = 1000g
}

# ─────────────────────────────────────────────────────────────────────────────
#  NSE STOCK UNIVERSE  (21 stocks from big_boom.xls)
# ─────────────────────────────────────────────────────────────────────────────
NSE_STOCKS = [
    "ULTRACEMCO", "DIXON",      "APARINDS",  "BAJAJHLDNG", "BAJAJ-AUTO",
    "GILLETTE",   "APOLLOHOSP", "LINDEINDIA","OFSS",        "POLYCAB",
    "CRAFTSMAN",  "EICHERMOT",  "ATUL",      "AMBER",       "ABB",
    "NAVINFLUOR", "DIVISLAB",   "BRITANNIA", "ALKEM",       "PERSISTENT",
    "JKCEMENT",
]
MCX_SYMBOLS = ["GOLDTEN", "SILVERMICRO"]

NSE_SESSION_START = "09:15"
MCX_SESSION_START = "09:00"
MCX_SESSION_END   = "23:30"
LOOKBACK_DAYS     = 2

# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def build_headers(client_id: str, access_token: str) -> Dict[str, str]:
    return {
        "Accept":       "application/json",
        "Content-Type": "application/json",
        "access-token": access_token,
        "client-id":    client_id,
    }

def _instrument_type(seg: str) -> str:
    if seg == "NSE_EQ":   return "EQUITY"
    if seg == "MCX_COMM": return "FUTCOM"
    return "EQUITY"

def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")

def hhmm() -> str:
    return datetime.now().strftime("%H:%M")

def prompt(msg: str, default: str) -> str:
    try:
        val = input(f"  {msg} [{default}]: ").strip()
        return val if val else default
    except EOFError:
        return default

# ─────────────────────────────────────────────────────────────────────────────
#  HEIKIN ASHI ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def compute_ha(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    HA Close = (O + H + L + C) / 4
    HA Open  = (prev_HA_Open + prev_HA_Close) / 2   [first bar: (O+C)/2]
    HA High  = max(High, HA_Open, HA_Close)
    HA Low   = min(Low,  HA_Open, HA_Close)
    """
    ha: List[Dict[str, Any]] = []
    for i, c in enumerate(candles):
        o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        ha_close = (o + h + l + cl) / 4.0
        ha_open  = (ha[-1]["open"] + ha[-1]["close"]) / 2.0 if i > 0 else (o + cl) / 2.0
        ha.append({
            "bucket": c["bucket"],
            "open":   ha_open,
            "high":   max(h, ha_open, ha_close),
            "low":    min(l, ha_open, ha_close),
            "close":  ha_close,
        })
    return ha

def ha_color(candle: Dict[str, Any]) -> str:
    if candle["close"] > candle["open"]: return "GREEN"
    if candle["close"] < candle["open"]: return "RED"
    return "DOJI"

# ─────────────────────────────────────────────────────────────────────────────
#  DHAN REST — OHLC FETCH
# ─────────────────────────────────────────────────────────────────────────────

def fetch_ohlc(
    client_id: str,
    access_token: str,
    security_id: str,
    exchange_segment: str,
    interval: str,
) -> List[Dict[str, Any]]:
    now   = datetime.now()
    start = now - timedelta(days=LOOKBACK_DAYS)

    payload = {
        "securityId":      str(security_id),
        "exchangeSegment": exchange_segment,
        "instrument":      _instrument_type(exchange_segment),
        "interval":        interval,
        "oi":              False,
        "fromDate":        start.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate":          now.strftime("%Y-%m-%d %H:%M:%S"),
    }
    resp = requests.post(
        INTRADAY_URL,
        headers=build_headers(client_id, access_token),
        json=payload,
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json()

    opens      = data.get("open",      [])
    highs      = data.get("high",      [])
    lows       = data.get("low",       [])
    closes     = data.get("close",     [])
    volumes    = data.get("volume",    [])
    timestamps = data.get("timestamp", [])

    n = min(len(opens), len(highs), len(lows), len(closes), len(timestamps))
    candles = [
        {
            "bucket": int(timestamps[i]),
            "open":   float(opens[i]),
            "high":   float(highs[i]),
            "low":    float(lows[i]),
            "close":  float(closes[i]),
            "volume": float(volumes[i]) if i < len(volumes) else 0.0,
        }
        for i in range(n)
    ]
    candles.sort(key=lambda x: x["bucket"])

    # Drop current incomplete candle
    interval_sec   = int(interval) * 60
    current_bucket = (int(time.time()) // interval_sec) * interval_sec
    if candles and candles[-1]["bucket"] >= current_bucket:
        candles = candles[:-1]

    return candles

# ─────────────────────────────────────────────────────────────────────────────
#  DHAN REST — ORDER PLACEMENT
# ─────────────────────────────────────────────────────────────────────────────

def place_market_order(
    client_id: str,
    access_token: str,
    security_id: str,
    exchange_segment: str,
    transaction_type: str,
    quantity: int,
    product_type: str,
) -> Dict[str, Any]:
    payload = {
        "dhanClientId":      client_id,
        "transactionType":   transaction_type,
        "exchangeSegment":   exchange_segment,
        "productType":       product_type,
        "orderType":         "MARKET",
        "validity":          "DAY",
        "securityId":        str(security_id),
        "quantity":          int(quantity),
        "price":             0,
        "disclosedQuantity": 0,
        "afterMarketOrder":  False,
    }
    resp = requests.post(
        ORDER_URL,
        headers=build_headers(client_id, access_token),
        json=payload,
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()

def fetch_fill_price(client_id: str, access_token: str, order_id: str, fallback: float) -> float:
    try:
        resp = requests.get(
            f"{ORDER_STATUS_BASE_URL}/{order_id}",
            headers=build_headers(client_id, access_token),
            timeout=10,
        )
        resp.raise_for_status()
        d = resp.json()
        p = float(d.get("averageTradedPrice") or d.get("price") or fallback)
        return p if p > 0 else fallback
    except Exception:
        return fallback

# ─────────────────────────────────────────────────────────────────────────────
#  INSTRUMENT MASTER CSV  (cached locally — avoids 50MB download every run)
# ─────────────────────────────────────────────────────────────────────────────

def load_master_csv() -> List[Dict[str, str]]:
    use_cache = (
        MASTER_CSV_CACHE.exists()
        and (time.time() - MASTER_CSV_CACHE.stat().st_mtime) < MASTER_CACHE_MAX_AGE_H * 3600
    )

    if use_cache:
        age_h = (time.time() - MASTER_CSV_CACHE.stat().st_mtime) / 3600
        print(f"  {green('✓')} Using cached master CSV  ({age_h:.1f}h old)  →  {MASTER_CSV_CACHE}")
        with open(MASTER_CSV_CACHE, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            return [{k.strip(): (v.strip() if isinstance(v, str) else v)
                     for k, v in row.items()} for row in reader]

    print("  Downloading Dhan instrument master CSV  (~50 MB) — please wait ...")
    resp = requests.get(INSTRUMENT_MASTER_URL, timeout=120, stream=True)
    resp.raise_for_status()

    total      = int(resp.headers.get("content-length", 0))
    content    = b""
    downloaded = 0

    for chunk in resp.iter_content(chunk_size=131072):
        content    += chunk
        downloaded += len(chunk)
        mb = downloaded / 1_048_576
        if total:
            pct = downloaded / total * 100
            print(f"\r    {mb:.1f} MB / {total/1_048_576:.1f} MB  ({pct:.0f}%)", end="", flush=True)
        else:
            print(f"\r    {mb:.1f} MB downloaded ...", end="", flush=True)

    print()   # newline after progress bar
    MASTER_CSV_CACHE.write_bytes(content)
    print(f"  {green('✓')} Saved to cache → {MASTER_CSV_CACHE}")

    text   = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [{k.strip(): (v.strip() if isinstance(v, str) else v)
             for k, v in row.items()} for row in reader]

# ─────────────────────────────────────────────────────────────────────────────
#  INSTRUMENT RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def resolve_nse_stocks(rows: List[Dict[str, str]], symbols: List[str]) -> Dict[str, str]:
    """One-pass index build → O(n) total, not O(n × m)."""
    print("  Building NSE_EQ index ...")
    index: Dict[str, str] = {}
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID", "").strip().upper() != "NSE":
            continue
        if row.get("SEM_SEGMENT", "").strip().upper() != "E":
            continue
        sym = row.get("SEM_TRADING_SYMBOL", "").strip().upper()
        sid = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()
        if sym and sid:
            index[sym] = sid

    result: Dict[str, str] = {}
    for sym in symbols:
        sid = index.get(sym.upper())
        if sid:
            result[sym] = sid
            print(f"    {green('✓')} {sym:<15}  sid={sid}")
        else:
            print(f"    {yellow('✗')} {sym:<15}  NOT FOUND — will skip")
    return result


def resolve_mcx_future(
    rows: List[Dict[str, str]],
    prefix: str,
    allow_pick: bool = True,
) -> Optional[Dict[str, str]]:
    """
    Find MCX FUTURES contracts only. Excludes options (CE/PE).
    Shows active contracts and lets user pick by number.
    """
    today = datetime.now().date()

    # Strict prefixes — avoid short ones that match options too broadly
    variants = list(dict.fromkeys([
        prefix.upper(),
        prefix.upper().replace("MICRO", "MIC"),
    ]))
    variants = [v for v in variants if len(v) >= 7]

    expiry_fields = ["SEM_EXPIRY_DATE", "SEM_EXPIRY_FLAG", "ExpiryDate", "expiry_date"]
    found:      list = []   # (expiry_dt, sid, trading_sym)
    mcx_samples:list = []

    for row in rows:
        exch = row.get("SEM_EXM_EXCH_ID", "").strip().upper()
        seg  = row.get("SEM_SEGMENT",     "").strip().upper()
        if exch != "MCX" or seg not in ("M", "COMM", "MCX"):
            continue

        trading_sym = row.get("SEM_TRADING_SYMBOL", "").strip().upper()
        sym_name    = row.get("SM_SYMBOL_NAME",     "").strip().upper()
        instrument  = row.get("SEM_INSTRUMENT_NAME","").strip().upper()
        sid         = str(row.get("SEM_SMST_SECURITY_ID", "")).strip()

        if len(mcx_samples) < 5:
            mcx_samples.append(
                f"SYM={sym_name!r:20} TRADING={trading_sym!r:35} INSTR={instrument!r}"
            )

        if not sid:
            continue

        # ── FUTURES filter ────────────────────────────────────
        # Keep only -FUT contracts; reject CE/PE options
        is_future = (trading_sym.endswith("-FUT")
                     or trading_sym.endswith("FUT")
                     or "FUTCOM" in instrument
                     or instrument == "FUTCOM")
        is_option = (trading_sym.endswith("-CE")
                     or trading_sym.endswith("-PE")
                     or "-CE-" in trading_sym
                     or "-PE-" in trading_sym
                     or instrument in ("OPTFUT", "OPTCOM"))
        if is_option or not is_future:
            continue

        # ── Prefix match ──────────────────────────────────────
        all_names = f"{trading_sym} {sym_name}"
        if not any(v in all_names for v in variants):
            continue

        # ── Parse expiry ──────────────────────────────────────
        expiry_dt = None
        for field in expiry_fields:
            expiry_str = row.get(field, "").strip()
            if not expiry_str:
                continue
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y",
                        "%Y/%m/%d", "%d-%b-%Y", "%d %b %Y"):
                try:
                    expiry_dt = datetime.strptime(expiry_str[:11], fmt).date()
                    break
                except Exception:
                    pass
            if expiry_dt:
                break

        found.append((expiry_dt, sid, trading_sym))

    if not found:
        print(f"    NOT FOUND: {prefix}  (tried: {variants})")
        print(f"    Sample MCX rows from CSV:")
        for s in mcx_samples:
            print(f"      {s}")
        return None

    active  = sorted(
        [(e, s, t) for e, s, t in found if e and e >= today],
        key=lambda x: x[0]
    )
    no_exp  = [(e, s, t) for e, s, t in found if not e]
    ordered = active + no_exp

    if not ordered:
        expired = [(e, s, t) for e, s, t in found if e and e < today]
        print(f"    Only expired futures for {prefix}:")
        for e, s, t in expired[:3]:
            print(f"      contract={t}  expiry={e}  sid={s}")
        return None

    print(f"    Found {len(ordered)} active future(s) for {bold(prefix)}:")
    for i, (e, s, t) in enumerate(ordered[:6]):
        exp_str = str(e) if e else "unknown"
        marker  = green(f"  [{i+1}]") if i == 0 else grey(f"  [{i+1}]")
        print(f"    {marker} contract={cyan(t):<38} expiry={exp_str:<12} sid={s}")

    if len(ordered) == 1 or not allow_pick:
        chosen = ordered[0]
    else:
        try:
            raw = input(f"      Pick contract [1-{min(len(ordered), 6)}] (Enter = nearest): ").strip()
            idx = int(raw) - 1 if raw.isdigit() else 0
            idx = max(0, min(idx, len(ordered) - 1))
        except Exception:
            idx = 0
        chosen = ordered[idx]

    e, sid, trading_sym = chosen
    expiry_out = str(e) if e else "unknown"
    print(f"    {green(chr(10003))} {prefix:<15} -> sid={sid}  "
          f"contract={cyan(trading_sym)}  expiry={expiry_out}")
    return {"security_id": sid, "trading_symbol": trading_sym, "expiry": expiry_out}


# ─────────────────────────────────────────────────────────────────────────────
#  DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class InstrumentConfig:
    name:             str
    exchange_segment: str
    security_id:      str
    product_type:     str
    lot_multiplier:   int = 1
    trading_symbol:   str = ""
    expiry:           str = ""

    @property
    def is_mcx(self) -> bool:
        return self.exchange_segment == "MCX_COMM"


@dataclass
class InstrumentState:
    config:       InstrumentConfig
    api_qty:      int   = 0       # Dhan API quantity (lot_multiplier already applied)

    # Position
    position:     str   = "FLAT"
    entry_price:  float = 0.0
    entry_time:   str   = ""

    # Market data
    last_ltp:     float = 0.0
    ha_open:      float = 0.0
    ha_close:     float = 0.0
    color:        str   = "-"
    last_signal:  str   = "-"
    bar_time:     str   = ""

    # Control
    skip:         bool  = False
    sq_off_done:  bool  = False

    # Status
    status:       str   = "Waiting..."
    last_update:  str   = ""

    @property
    def unrealized_pnl(self) -> float:
        if self.position == "FLAT" or not self.entry_price or not self.last_ltp:
            return 0.0
        diff = (self.last_ltp - self.entry_price) if self.position == "LONG" \
               else (self.entry_price - self.last_ltp)
        return diff * self.api_qty

    @property
    def user_qty(self) -> int:
        if self.config.lot_multiplier <= 1:
            return self.api_qty
        return self.api_qty // self.config.lot_multiplier if self.api_qty else 0


# ─────────────────────────────────────────────────────────────────────────────
#  LIVE TICKER — WebSocket (one connection, all instruments)
# ─────────────────────────────────────────────────────────────────────────────

class MultiTickerWS:
    """
    Single Dhan WebSocket connection that subscribes to ALL instruments at once.
    Calls on_tick(security_id: str, ltp: float) on every price update.
    Runs in its own daemon thread with auto-reconnect.
    """

    def __init__(
        self,
        client_id:    str,
        access_token: str,
        instruments:  list,        # list of (security_id, exchange_segment)
        on_tick,                   # Callable[[str, float], None]
        on_status=None,            # Callable[[str], None]
    ):
        self.client_id    = str(client_id).strip()
        self.access_token = str(access_token).strip()
        self.instruments  = instruments
        self._on_tick_cb  = on_tick
        self._on_status_cb = on_status
        self._stop        = threading.Event()
        self._ws          = None
        self._last_keys:  Dict[str, tuple] = {}
        self.status:      str = "Not started"
        self.tick_count:  int = 0

    @property
    def _url(self) -> str:
        return (
            f"wss://api-feed.dhan.co?version=2"
            f"&token={self.access_token}"
            f"&clientId={self.client_id}"
            f"&authType=2"
        )

    def _status(self, msg: str):
        self.status = msg
        if self._on_status_cb:
            try: self._on_status_cb(msg)
            except Exception: pass

    def _on_open(self, ws):
        sub = {
            "RequestCode":     REQ_SUB_TICKER,
            "InstrumentCount": len(self.instruments),
            "InstrumentList": [
                {"ExchangeSegment": seg, "SecurityId": sid}
                for sid, seg in self.instruments
            ],
        }
        ws.send(json.dumps(sub))
        self._status(f"WS connected — subscribed {len(self.instruments)} instruments")

    def _on_message(self, ws, message):
        try:
            if isinstance(message, str):
                return
            hdr = parse_header_8(bytes(message))
            if not hdr or int(hdr["resp_code"]) != RESP_TICKER:
                return
            t = parse_ticker(hdr["payload"])
            if not t:
                return

            sid = str(hdr["security_id"])
            ltp = float(t["ltp"])
            ltt = int(t["ltt_epoch"])

            # Deduplicate identical packets
            key = (round(ltp, 8), ltt)
            if self._last_keys.get(sid) == key:
                return
            self._last_keys[sid] = key

            self.tick_count += 1
            self._on_tick_cb(sid, ltp)

        except Exception as e:
            self._status(f"WS parse err: {e}")

    def _on_error(self, ws, error):
        self._status(f"WS error: {error}")

    def _on_close(self, ws, code, msg):
        self._status(f"WS closed (code={code})")

    def _run(self):
        websocket.enableTrace(False)
        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    self._url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self._status(f"WS exception: {e}")
            if not self._stop.is_set():
                self._status("WS reconnecting in 3s ...")
                time.sleep(3)

    def start(self):
        self._stop.clear()
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._stop.set()
        if self._ws_client:
            self._ws_client.stop()
    # ── WebSocket live LTP ────────────────────────────────────

    def _start_ws(self):
        """Start one WebSocket connection subscribing to all instruments."""
        self._sid_map = {st.config.security_id: st for st in self.instruments}
        instr_list    = [
            (st.config.security_id, st.config.exchange_segment)
            for st in self.instruments
        ]
        def _on_status(msg):
            self.ws_status = msg
            self._log(f"[WS] {msg}")

        self._ws_client = MultiTickerWS(
            client_id=self.client_id,
            access_token=self.access_token,
            instruments=instr_list,
            on_tick=self._on_ws_tick,
            on_status=_on_status,
        )
        self._ws_client.start()

    def _on_ws_tick(self, security_id: str, ltp: float):
        """Called on every live tick — update LTP immediately."""
        st = self._sid_map.get(security_id)
        if st:
            with self.lock:
                st.last_ltp = round(ltp, 2)
            self.ws_ticks += 1


        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
#  STRATEGY ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class StrategyEngine:

    def __init__(
        self,
        client_id:    str,
        access_token: str,
        instruments:  List[InstrumentState],
        interval:     str,
        nse_sq_time:  str,
        mcx_sq_time:  str,
        paper_mode:   bool = True,
    ):
        self.client_id    = client_id
        self.access_token = access_token
        self.instruments  = instruments
        self.interval     = interval
        self.nse_sq_time  = nse_sq_time
        self.mcx_sq_time  = mcx_sq_time
        self.paper_mode   = paper_mode
        self.lock         = threading.RLock()
        self._stop        = threading.Event()
        self._startup_done: Dict[str, bool] = {}
        self.next_poll_at: str = "-"
        self._log_lines:  List[str] = []
        self.ws_status:   str = "Not started"
        self.ws_ticks:    int = 0
        self._sid_map:    Dict[str, InstrumentState] = {}
        self._ws_client: Optional[MultiTickerWS] = None

    def start(self):
        self._stop.clear()
        threading.Thread(target=self._run, daemon=True).start()
        self._start_ws()

    def stop(self):
        self._stop.set()
        if self._ws_client:
            self._ws_client.stop()
    # ── WebSocket live LTP ────────────────────────────────────

    def _start_ws(self):
        """Start one WebSocket connection subscribing to all instruments."""
        self._sid_map = {st.config.security_id: st for st in self.instruments}
        instr_list    = [
            (st.config.security_id, st.config.exchange_segment)
            for st in self.instruments
        ]
        def _on_status(msg):
            self.ws_status = msg
            self._log(f"[WS] {msg}")

        self._ws_client = MultiTickerWS(
            client_id=self.client_id,
            access_token=self.access_token,
            instruments=instr_list,
            on_tick=self._on_ws_tick,
            on_status=_on_status,
        )
        self._ws_client.start()

    def _on_ws_tick(self, security_id: str, ltp: float):
        """Called on every live tick — update LTP immediately."""
        st = self._sid_map.get(security_id)
        if st:
            with self.lock:
                st.last_ltp = round(ltp, 2)
            self.ws_ticks += 1



    # ── Logging ───────────────────────────────────────────────

    def _log(self, msg: str):
        entry = f"[{now_str()}] {msg}"
        with self.lock:
            self._log_lines.append(entry)
            if len(self._log_lines) > 300:
                self._log_lines = self._log_lines[-300:]

    def get_logs(self, n: int = 15) -> List[str]:
        with self.lock:
            return list(self._log_lines[-n:])

    # ── Main loop ─────────────────────────────────────────────

    def _run(self):
        self._log("Engine started")
        self._poll_all(startup=True)
        self._set_next_poll()

        while not self._stop.is_set():
            self._wait_for_next_poll()
            if self._stop.is_set():
                break
            self._check_auto_squareoff()
            self._poll_all(startup=False)
            self._set_next_poll()

        self._log("Engine stopped")

    def _set_next_poll(self):
        iv_sec           = int(self.interval) * 60
        next_bucket      = ((int(time.time()) // iv_sec) + 1) * iv_sec
        self.next_poll_at = datetime.fromtimestamp(next_bucket + 3).strftime("%H:%M:%S")

    def _wait_for_next_poll(self):
        iv_sec       = int(self.interval) * 60
        now_ts       = time.time()
        next_bucket  = ((int(now_ts) // iv_sec) + 1) * iv_sec
        wait         = (next_bucket + 3) - now_ts
        if wait < 0:
            wait = 0.5
        elapsed = 0.0
        while elapsed < wait and not self._stop.is_set():
            time.sleep(min(1.0, wait - elapsed))
            elapsed += 1.0

    def _check_auto_squareoff(self):
        t = hhmm()
        for st in self.instruments:
            if st.sq_off_done:
                continue
            sq_t = self.mcx_sq_time if st.config.is_mcx else self.nse_sq_time
            if t >= sq_t and st.position != "FLAT":
                self._log(f"AUTO SQ-OFF {st.config.name}")
                self._close_position(st, "AutoSqOff")
                with self.lock:
                    st.sq_off_done = True

    def _in_session(self, cfg: InstrumentConfig) -> bool:
        t = hhmm()
        return (MCX_SESSION_START <= t < MCX_SESSION_END) if cfg.is_mcx \
               else (NSE_SESSION_START <= t < "15:31")

    # ── Poll ──────────────────────────────────────────────────

    def _poll_all(self, startup: bool = False):
        for st in self.instruments:
            if self._stop.is_set():
                break
            if st.skip or st.sq_off_done:
                continue
            if not self._in_session(st.config):
                with self.lock:
                    st.status = "Mkt Closed"
                continue
            try:
                self._process(st, startup)
            except Exception as e:
                self._log(f"ERROR {st.config.name}: {e}")
                with self.lock:
                    st.status = f"Err: {e}"
            time.sleep(0.3)

    def _process(self, st: InstrumentState, startup: bool):
        candles = fetch_ohlc(
            self.client_id, self.access_token,
            st.config.security_id, st.config.exchange_segment, self.interval,
        )
        if len(candles) < 2:
            with self.lock:
                st.status = f"Too few bars ({len(candles)})"
            return

        ha        = compute_ha(candles)
        last_ha   = ha[-1]
        color     = ha_color(last_ha)
        ltp       = candles[-1]["close"]
        bar_ts    = datetime.fromtimestamp(last_ha["bucket"]).strftime("%H:%M")

        with self.lock:
            st.ha_open    = round(last_ha["open"],  2)
            st.ha_close   = round(last_ha["close"], 2)
            st.color      = color
            st.last_ltp   = round(ltp, 2)
            st.bar_time   = bar_ts
            st.last_update = now_str()

        if color == "DOJI":
            with self.lock:
                st.status = "DOJI — holding"
            return

        signal = "BUY" if color == "GREEN" else "SELL"
        with self.lock:
            st.last_signal = signal

        key = st.config.name

        # Startup: enter immediately
        if startup and not self._startup_done.get(key):
            self._startup_done[key] = True
            self._log(f"STARTUP {key} → {signal} (HA={color})")
            if st.position == "FLAT":
                self._open_position(st, signal)
            elif (signal == "BUY" and st.position != "LONG") or \
                 (signal == "SELL" and st.position != "SHORT"):
                self._reverse_position(st, signal)
            return
        if startup:
            return

        # Running: act on direction change only
        if signal == "BUY" and st.position != "LONG":
            self._log(f"SIGNAL {key} GREEN HA@{bar_ts} → BUY")
            self._open_position(st, "BUY") if st.position == "FLAT" \
                else self._reverse_position(st, "BUY")

        elif signal == "SELL" and st.position != "SHORT":
            self._log(f"SIGNAL {key} RED HA@{bar_ts} → SELL")
            self._open_position(st, "SELL") if st.position == "FLAT" \
                else self._reverse_position(st, "SELL")
        else:
            with self.lock:
                arrow = "↑LONG" if st.position == "LONG" else "↓SHORT"
                st.status = f"Holding {arrow}"

    # ── Orders ────────────────────────────────────────────────

    def _open_position(self, st: InstrumentState, direction: str):
        side = "BUY" if direction == "BUY" else "SELL"
        fill = st.last_ltp

        # ── PAPER MODE — simulate fill at current LTP ─────────
        if self.paper_mode:
            self._log(f"[PAPER] {st.config.name} {side} qty={st.api_qty} @ {fill:.2f}")
            with self.lock:
                st.position    = "LONG" if direction == "BUY" else "SHORT"
                st.entry_price = fill
                st.entry_time  = now_str()
                st.status      = f"[P] {st.position} @{fill:.2f}"
            return

        # ── LIVE MODE ─────────────────────────────────────────
        try:
            resp     = place_market_order(
                self.client_id, self.access_token,
                st.config.security_id, st.config.exchange_segment,
                side, st.api_qty, st.config.product_type,
            )
            order_id = str(resp.get("orderId") or resp.get("order_id") or "")
            self._log(f"[LIVE] ORDER {st.config.name} {side} qty={st.api_qty} → id={order_id}")

            if order_id:
                time.sleep(1.5)
                fill = fetch_fill_price(self.client_id, self.access_token, order_id, st.last_ltp)

            with self.lock:
                st.position    = "LONG" if direction == "BUY" else "SHORT"
                st.entry_price = fill
                st.entry_time  = now_str()
                st.status      = f"Entered {st.position} @{fill:.2f}"

        except Exception as e:
            body = getattr(getattr(e, "response", None), "text", "")
            self._log(f"ORDER ERR {st.config.name} {side}: {e} | body={body[:200]}")
            with self.lock:
                st.status = f"OrderErr: {body[:40]}" if body else "OrderErr"

    def _close_position(self, st: InstrumentState, reason: str = ""):
        if st.position == "FLAT":
            return
        side = "SELL" if st.position == "LONG" else "BUY"

        # ── PAPER MODE ────────────────────────────────────────
        if self.paper_mode:
            pnl = st.unrealized_pnl
            self._log(f"[PAPER] CLOSE {st.config.name} {side} ({reason})  P&L=₹{pnl:+.2f}")
            with self.lock:
                st.position    = "FLAT"
                st.entry_price = 0.0
                st.entry_time  = ""
                st.status      = f"[P] Closed ({reason})"
            return

        # ── LIVE MODE ─────────────────────────────────────────
        try:
            resp     = place_market_order(
                self.client_id, self.access_token,
                st.config.security_id, st.config.exchange_segment,
                side, st.api_qty, st.config.product_type,
            )
            order_id = str(resp.get("orderId") or resp.get("order_id") or "")
            self._log(f"[LIVE] CLOSE {st.config.name} {side} ({reason}) → id={order_id}")
            with self.lock:
                st.position    = "FLAT"
                st.entry_price = 0.0
                st.entry_time  = ""
                st.status      = f"Closed ({reason})"
        except Exception as e:
            body = getattr(getattr(e, "response", None), "text", "")
            self._log(f"CLOSE ERR {st.config.name}: {e} | body={body[:200]}")
            with self.lock:
                st.status = "CloseErr"

    def _reverse_position(self, st: InstrumentState, new_dir: str):
        self._close_position(st, "Reversal")
        time.sleep(0.5)
        self._open_position(st, new_dir)


# ─────────────────────────────────────────────────────────────────────────────
#  GUI APPLICATION  (CustomTkinter dark theme)
# ─────────────────────────────────────────────────────────────────────────────

import customtkinter as ctk

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

# Palette
C_GREEN  = "#1db954"
C_RED    = "#e05252"
C_YELLOW = "#f0a500"
C_BLUE   = "#4a9eff"
C_GRAY   = "#888888"
C_BG     = "#0f0f1a"
C_FRAME  = "#16213e"
C_ROW_A  = "#131a2e"
C_ROW_B  = "#0f1520"
C_HEADER = "#1a2540"

# ── Column definitions ────────────────────────────────────────────────────────
COLS = [
    ("#",        35),  ("Symbol",   145), ("Exch",    55),
    ("HA Open",  88),  ("HA Close",  88), ("Color",   72),
    ("Signal",   62),  ("Position",  74), ("Entry ₹", 90),
    ("Qty",      46),  ("LTP ₹",    90),  ("P&L ₹",  95),
    ("Bar",      54),  ("Status",  185),
]


class InstrumentRow:
    """One row of widgets inside the scrollable frame."""

    def __init__(self, parent, idx: int, st: InstrumentState, bg: str):
        self.st   = st
        self._bg  = bg
        self.frame = ctk.CTkFrame(parent, fg_color=bg, height=30, corner_radius=3)
        self.frame.pack(fill="x", padx=2, pady=1)
        self.frame.pack_propagate(False)

        self._labels: list = []
        x = 4

        for col_idx, (_, w) in enumerate(COLS):
            lbl = ctk.CTkLabel(
                self.frame, text="-", width=w, anchor="center",
                font=ctk.CTkFont(size=11), text_color=C_GRAY,
            )
            lbl.place(x=x, rely=0.5, anchor="w")
            self._labels.append(lbl)
            x += w + 2

        # Index label
        self._labels[0].configure(text=str(idx), text_color=C_GRAY)
        # Symbol
        self._labels[1].configure(
            text=st.config.name, text_color="white",
            font=ctk.CTkFont(size=11, weight="bold"), anchor="w",
        )
        # Exchange badge
        exch_color = C_YELLOW if st.config.is_mcx else C_BLUE
        self._labels[2].configure(
            text="MCX" if st.config.is_mcx else "NSE", text_color=exch_color,
        )

    def update(self, nse_qty: int, gold_lots: int, silver_lots: int):
        st = self.st

        # HA Open
        self._labels[3].configure(
            text=f"{st.ha_open:.2f}" if st.ha_open else "-",
            text_color="white",
        )
        # HA Close
        self._labels[4].configure(
            text=f"{st.ha_close:.2f}" if st.ha_close else "-",
            text_color="white",
        )
        # Color
        col_map = {"GREEN": C_GREEN, "RED": C_RED, "DOJI": C_YELLOW}
        self._labels[5].configure(
            text=st.color if st.color != "-" else "-",
            text_color=col_map.get(st.color, C_GRAY),
        )
        # Signal
        sig_map = {"BUY": C_GREEN, "SELL": C_RED}
        self._labels[6].configure(
            text=st.last_signal,
            text_color=sig_map.get(st.last_signal, C_GRAY),
        )
        # Position
        pos_map = {"LONG": C_GREEN, "SHORT": C_RED, "FLAT": C_GRAY}
        self._labels[7].configure(
            text=st.position,
            text_color=pos_map.get(st.position, C_GRAY),
        )
        # Entry
        self._labels[8].configure(
            text=f"{st.entry_price:.2f}" if st.entry_price else "-",
            text_color="white",
        )
        # Qty
        if st.position != "FLAT":
            qty_display = str(st.user_qty)
        else:
            if st.config.is_mcx:
                qty_display = str(gold_lots if st.config.name == "GOLDTEN" else silver_lots)
            else:
                qty_display = str(nse_qty)
        self._labels[9].configure(text=qty_display, text_color=C_GRAY)

        # LTP
        self._labels[10].configure(
            text=f"{st.last_ltp:.2f}" if st.last_ltp else "-",
            text_color="white",
        )
        # P&L
        pnl = st.unrealized_pnl
        if st.position == "FLAT":
            self._labels[11].configure(text="-", text_color=C_GRAY)
        else:
            pnl_color = C_GREEN if pnl >= 0 else C_RED
            self._labels[11].configure(
                text=f"₹{pnl:+.2f}", text_color=pnl_color,
            )
        # Bar time
        self._labels[12].configure(
            text=st.bar_time or "-", text_color=C_GRAY,
        )
        # Status
        s = st.status[:32]
        sc = C_GRAY
        if "Err" in s:                sc = C_RED
        elif "[P] LONG" in s or "Entered LONG" in s:  sc = C_GREEN
        elif "[P] SHORT" in s or "Entered SHORT" in s: sc = C_RED
        elif "Holding ↑" in s:       sc = C_GREEN
        elif "Holding ↓" in s:       sc = C_RED
        elif "Closed" in s:          sc = C_YELLOW
        elif "Mkt Closed" in s:      sc = C_GRAY
        self._labels[13].configure(text=s, text_color=sc)


class HATradingApp(ctk.CTk):

    def __init__(self):
        super().__init__()
        self.title("Balfund  HA + SMA(1)  Strategy Trader  v1.0")
        self.geometry("1720x920")
        self.minsize(1400, 700)
        self.configure(fg_color=C_BG)

        # State
        self.engine:      Optional[StrategyEngine] = None
        self.instruments: List[InstrumentState]    = []
        self.running:     bool = False
        self._rows:       List[InstrumentRow] = []

        # Config vars
        self.interval_var  = ctk.StringVar(value="5")
        self.nse_qty_var   = ctk.IntVar(value=10)
        self.gold_lots_var = ctk.IntVar(value=1)
        self.silv_lots_var = ctk.IntVar(value=1)
        self.nse_sq_var    = ctk.StringVar(value="15:15")
        self.mcx_sq_var    = ctk.StringVar(value="23:25")
        self.paper_var     = ctk.BooleanVar(value=True)

        self._build_ui()
        self.after(1500, self._gui_tick)

    # ─────────────────────────────────────────────────────────
    # UI BUILD
    # ─────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── Top bar ──────────────────────────────────────────
        top = ctk.CTkFrame(self, fg_color=C_HEADER, height=52, corner_radius=0)
        top.pack(fill="x")
        top.pack_propagate(False)

        ctk.CTkLabel(
            top,
            text="📈  BALFUND   HA + SMA(1)  STRATEGY TRADER",
            font=ctk.CTkFont(size=17, weight="bold"),
            text_color=C_BLUE,
        ).pack(side="left", padx=18)

        self.pnl_lbl = ctk.CTkLabel(
            top, text="Net P&L:  ₹0.00",
            font=ctk.CTkFont(size=14, weight="bold"),
            text_color=C_YELLOW,
        )
        self.pnl_lbl.pack(side="right", padx=18)

        self.clock_lbl = ctk.CTkLabel(
            top, text="", font=ctk.CTkFont(size=12), text_color=C_GRAY,
        )
        self.clock_lbl.pack(side="right", padx=12)

        self.ws_lbl = ctk.CTkLabel(
            top, text="WS: —", font=ctk.CTkFont(size=11), text_color=C_GRAY,
        )
        self.ws_lbl.pack(side="right", padx=12)

        self.mode_lbl = ctk.CTkLabel(
            top, text="● PAPER", font=ctk.CTkFont(size=12, weight="bold"),
            text_color=C_YELLOW,
        )
        self.mode_lbl.pack(side="right", padx=12)

        # ── Tabs ─────────────────────────────────────────────
        self.tabs = ctk.CTkTabview(self, fg_color=C_BG, segmented_button_selected_color=C_BLUE)
        self.tabs.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        self.tabs.add("⚙  Settings")
        self.tabs.add("📊  Live Strategy")
        self.tabs.add("📋  Log")

        self._build_settings(self.tabs.tab("⚙  Settings"))
        self._build_strategy(self.tabs.tab("📊  Live Strategy"))
        self._build_log(self.tabs.tab("📋  Log"))

    # ── SETTINGS TAB ─────────────────────────────────────────

    def _build_settings(self, parent):
        parent.configure(fg_color=C_BG)
        parent.grid_columnconfigure((0, 1, 2, 3), weight=1)

        # Timeframe
        tf = ctk.CTkFrame(parent, fg_color=C_FRAME, corner_radius=10)
        tf.grid(row=0, column=0, padx=10, pady=12, sticky="nsew")
        ctk.CTkLabel(tf, text="Candle Timeframe",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=C_BLUE).pack(pady=(14, 8))
        for val, label in [("1","1 Minute"), ("5","5 Minutes"), ("15","15 Minutes")]:
            ctk.CTkRadioButton(tf, text=label, variable=self.interval_var,
                               value=val, font=ctk.CTkFont(size=12)).pack(
                anchor="w", padx=22, pady=5)
        tf.pack_propagate(False)

        # Quantity
        qf = ctk.CTkFrame(parent, fg_color=C_FRAME, corner_radius=10)
        qf.grid(row=0, column=1, padx=10, pady=12, sticky="nsew")
        ctk.CTkLabel(qf, text="Quantity per Trade",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=C_BLUE).pack(pady=(14, 8))
        for label, var, unit in [
            ("NSE Stocks:", self.nse_qty_var, "shares"),
            ("GOLDTEN:",    self.gold_lots_var, "lots"),
            ("SILVERMICRO:", self.silv_lots_var, "lots"),
        ]:
            row = ctk.CTkFrame(qf, fg_color="transparent")
            row.pack(fill="x", padx=16, pady=5)
            ctk.CTkLabel(row, text=label, width=130, anchor="w",
                         font=ctk.CTkFont(size=12)).pack(side="left")
            ctk.CTkEntry(row, textvariable=var, width=70,
                         font=ctk.CTkFont(size=12)).pack(side="left", padx=6)
            ctk.CTkLabel(row, text=unit, text_color=C_GRAY,
                         font=ctk.CTkFont(size=11)).pack(side="left")

        # Square-off
        sf = ctk.CTkFrame(parent, fg_color=C_FRAME, corner_radius=10)
        sf.grid(row=0, column=2, padx=10, pady=12, sticky="nsew")
        ctk.CTkLabel(sf, text="Auto Square-off",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=C_BLUE).pack(pady=(14, 8))
        for label, var in [("NSE (HH:MM):", self.nse_sq_var),
                           ("MCX (HH:MM):", self.mcx_sq_var)]:
            row = ctk.CTkFrame(sf, fg_color="transparent")
            row.pack(fill="x", padx=16, pady=8)
            ctk.CTkLabel(row, text=label, width=120, anchor="w",
                         font=ctk.CTkFont(size=12)).pack(side="left")
            ctk.CTkEntry(row, textvariable=var, width=90,
                         font=ctk.CTkFont(size=13)).pack(side="left", padx=6)
        ctk.CTkLabel(sf, text="NSE: 09:15 – 15:30\nMCX: 09:00 – 23:30",
                     text_color=C_GRAY, font=ctk.CTkFont(size=11),
                     justify="left").pack(padx=16, pady=6, anchor="w")

        # Mode
        mf = ctk.CTkFrame(parent, fg_color=C_FRAME, corner_radius=10)
        mf.grid(row=0, column=3, padx=10, pady=12, sticky="nsew")
        ctk.CTkLabel(mf, text="Trading Mode",
                     font=ctk.CTkFont(size=13, weight="bold"),
                     text_color=C_BLUE).pack(pady=(14, 12))
        ctk.CTkSwitch(
            mf, text="Paper Mode (safe)",
            variable=self.paper_var,
            font=ctk.CTkFont(size=12),
            onvalue=True, offvalue=False,
            progress_color=C_YELLOW,
        ).pack(padx=22, pady=4, anchor="w")
        self.paper_warn = ctk.CTkLabel(
            mf, text="⚠  Switch OFF for live orders",
            text_color=C_RED, font=ctk.CTkFont(size=11),
        )
        self.paper_warn.pack(padx=22, pady=4, anchor="w")

        # Control buttons
        btn_row = ctk.CTkFrame(parent, fg_color="transparent")
        btn_row.grid(row=1, column=0, columnspan=4, pady=16)

        self.start_btn = ctk.CTkButton(
            btn_row, text="▶  START STRATEGY",
            width=220, height=48,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=C_GREEN, hover_color="#17a844",
            command=self._on_start,
        )
        self.start_btn.pack(side="left", padx=12)

        self.stop_btn = ctk.CTkButton(
            btn_row, text="■  STOP",
            width=140, height=48,
            font=ctk.CTkFont(size=14, weight="bold"),
            fg_color=C_RED, hover_color="#c43a3a",
            command=self._on_stop, state="disabled",
        )
        self.stop_btn.pack(side="left", padx=12)

        self.status_lbl = ctk.CTkLabel(
            parent, text="Not started",
            text_color=C_GRAY, font=ctk.CTkFont(size=12),
        )
        self.status_lbl.grid(row=2, column=0, columnspan=4, pady=4)

    # ── STRATEGY TAB ─────────────────────────────────────────

    def _build_strategy(self, parent):
        parent.configure(fg_color=C_BG)

        # Column header bar
        hdr = ctk.CTkFrame(parent, fg_color=C_HEADER, height=30, corner_radius=4)
        hdr.pack(fill="x", padx=4, pady=(4, 0))
        hdr.pack_propagate(False)
        x = 4
        for name, w in COLS:
            ctk.CTkLabel(
                hdr, text=name, width=w, anchor="center",
                font=ctk.CTkFont(size=10, weight="bold"),
                text_color=C_BLUE,
            ).place(x=x, rely=0.5, anchor="w")
            x += w + 2

        # Per-instrument action bar (sq-off / skip)
        self.action_bar = ctk.CTkFrame(parent, fg_color=C_FRAME, height=36, corner_radius=4)
        self.action_bar.pack(fill="x", padx=4, pady=(2, 0))
        self.action_bar.pack_propagate(False)
        ctk.CTkLabel(
            self.action_bar, text="Select symbol then:", text_color=C_GRAY,
            font=ctk.CTkFont(size=11),
        ).pack(side="left", padx=10)

        self.sqoff_btn = ctk.CTkButton(
            self.action_bar, text="Manual Sq.Off",
            width=130, height=26,
            fg_color=C_RED, hover_color="#c43a3a",
            font=ctk.CTkFont(size=11),
            command=self._manual_sqoff,
        )
        self.sqoff_btn.pack(side="left", padx=6)

        self.skip_btn = ctk.CTkButton(
            self.action_bar, text="Toggle Skip",
            width=110, height=26,
            fg_color="#555555", hover_color="#333333",
            font=ctk.CTkFont(size=11),
            command=self._toggle_skip,
        )
        self.skip_btn.pack(side="left", padx=4)

        self.selected_sym_lbl = ctk.CTkLabel(
            self.action_bar, text="(no row selected)",
            text_color=C_GRAY, font=ctk.CTkFont(size=11),
        )
        self.selected_sym_lbl.pack(side="left", padx=10)

        # Summary bar
        self.summary_bar = ctk.CTkFrame(parent, fg_color=C_FRAME, height=28, corner_radius=4)
        self.summary_bar.pack(fill="x", padx=4, pady=(2, 0))
        self.summary_bar.pack_propagate(False)
        self.summary_lbl = ctk.CTkLabel(
            self.summary_bar,
            text="0 LONG   0 SHORT   Next poll: —",
            font=ctk.CTkFont(size=11), text_color=C_GRAY,
        )
        self.summary_lbl.pack(side="left", padx=10, pady=2)

        # Scrollable rows
        self.scroll = ctk.CTkScrollableFrame(parent, fg_color=C_BG, corner_radius=0)
        self.scroll.pack(fill="both", expand=True, padx=4, pady=4)

        self._selected_idx: Optional[int] = None

    def _build_instrument_rows(self):
        for w in self.scroll.winfo_children():
            w.destroy()
        self._rows.clear()
        self._selected_idx = None

        for idx, st in enumerate(self.instruments):
            bg = C_ROW_A if idx % 2 == 0 else C_ROW_B
            row = InstrumentRow(self.scroll, idx + 1, st, bg)
            # Click to select
            def _click(event, i=idx):
                self._select_row(i)
            row.frame.bind("<Button-1>", _click)
            for lbl in row._labels:
                lbl.bind("<Button-1>", _click)
            self._rows.append(row)

    def _select_row(self, idx: int):
        # Deselect old
        if self._selected_idx is not None and self._selected_idx < len(self._rows):
            old = self._rows[self._selected_idx]
            old.frame.configure(fg_color=old._bg)
        # Select new
        self._selected_idx = idx
        row = self._rows[idx]
        row.frame.configure(fg_color="#1a3a5c")
        self.selected_sym_lbl.configure(
            text=self.instruments[idx].config.name, text_color="white",
        )

    # ── LOG TAB ──────────────────────────────────────────────

    def _build_log(self, parent):
        parent.configure(fg_color=C_BG)
        self.log_box = ctk.CTkTextbox(
            parent,
            font=ctk.CTkFont(family="Courier", size=11),
            fg_color="#0a0a14",
            text_color="#cccccc",
        )
        self.log_box.pack(fill="both", expand=True, padx=6, pady=6)
        ctk.CTkButton(
            parent, text="Clear Log", width=100,
            command=lambda: self.log_box.delete("0.0", "end"),
        ).pack(side="right", padx=8, pady=4)

    def _append_log(self, msg: str):
        try:
            self.log_box.insert("end", msg + "\n")
            self.log_box.see("end")
        except Exception:
            pass

    # ─────────────────────────────────────────────────────────
    # BUTTON HANDLERS
    # ─────────────────────────────────────────────────────────

    def _on_start(self):
        self.start_btn.configure(state="disabled", text="Resolving...")
        self.status_lbl.configure(text="Fetching token & resolving instruments...",
                                  text_color=C_YELLOW)
        threading.Thread(target=self._resolve_and_start, daemon=True).start()

    def _on_stop(self):
        if self.engine:
            self.engine.stop()
        self.running = False
        self.start_btn.configure(state="normal", text="▶  START STRATEGY")
        self.stop_btn.configure(state="disabled")
        self.status_lbl.configure(text="Stopped", text_color=C_GRAY)
        self._append_log("[INFO] Strategy stopped by user.")

    def _manual_sqoff(self):
        if self._selected_idx is None or not self.engine:
            return
        name = self.instruments[self._selected_idx].config.name
        threading.Thread(
            target=self.engine.manual_squareoff, args=(name,), daemon=True,
        ).start()

    def _toggle_skip(self):
        if self._selected_idx is None:
            return
        st = self.instruments[self._selected_idx]
        st.skip = not st.skip
        label = f"SKIP: {st.config.name}" if st.skip else st.config.name
        self.selected_sym_lbl.configure(
            text=f"{label}  ({'skipped' if st.skip else 'active'})",
            text_color=C_YELLOW if st.skip else "white",
        )

    # ─────────────────────────────────────────────────────────
    # RESOLVE + START (background thread)
    # ─────────────────────────────────────────────────────────

    def _log_bg(self, msg: str):
        """Thread-safe log from background."""
        self.after(0, lambda m=msg: self._append_log(m))

    def _resolve_and_start(self):
        try:
            # Token
            self._log_bg("[1/4] Fetching token from Token Generator ...")
            client_id, access_token = fetch_token_from_generator()
            self._log_bg(f"      Client: {client_id}  Token: {access_token[:20]}...")

            # Master CSV
            self._log_bg("[2/4] Loading instrument master CSV ...")
            rows = load_master_csv()
            self._log_bg(f"      {len(rows):,} rows loaded.")

            # NSE stocks
            self._log_bg("[3/4] Resolving NSE stocks ...")
            nse_ids = resolve_nse_stocks(rows, NSE_STOCKS)
            instruments: List[InstrumentState] = []
            nse_qty = self.nse_qty_var.get()
            for sym in NSE_STOCKS:
                sid = nse_ids.get(sym)
                if not sid:
                    continue
                cfg_i = InstrumentConfig(
                    name=sym, exchange_segment="NSE_EQ",
                    security_id=sid, product_type="INTRADAY", lot_multiplier=1,
                )
                instruments.append(InstrumentState(config=cfg_i, api_qty=nse_qty))

            # MCX futures
            self._log_bg("[4/4] Resolving MCX futures ...")
            for sym, lots_var in [("GOLDTEN", self.gold_lots_var),
                                  ("SILVERMICRO", self.silv_lots_var)]:
                match = resolve_mcx_future(rows, sym, allow_pick=False)
                if not match:
                    self._log_bg(f"      WARNING: {sym} not resolved — skipping")
                    continue
                mult  = MCX_LOT_MULTIPLIERS[sym]
                cfg_i = InstrumentConfig(
                    name=sym, exchange_segment="MCX_COMM",
                    security_id=match["security_id"],
                    product_type="INTRADAY",
                    lot_multiplier=mult,
                    trading_symbol=match["trading_symbol"],
                    expiry=match["expiry"],
                )
                instruments.append(
                    InstrumentState(config=cfg_i, api_qty=lots_var.get() * mult)
                )

            self._log_bg(f"      {len(instruments)} instruments resolved.")

            if not instruments:
                self.after(0, lambda: self._on_start_error("No instruments resolved."))
                return

            self.instruments = instruments
            self.after(0, self._on_resolved)

        except Exception as e:
            self.after(0, lambda err=str(e): self._on_start_error(err))

    def _on_resolved(self):
        self._build_instrument_rows()

        paper = self.paper_var.get()
        self.engine = StrategyEngine(
            client_id="",  # engine reads via fetch_token_from_generator
            access_token="",
            instruments=self.instruments,
            interval=self.interval_var.get(),
            nse_sq_time=self.nse_sq_var.get(),
            mcx_sq_time=self.mcx_sq_var.get(),
            paper_mode=paper,
        )
        # Patch engine credentials from token fetch
        threading.Thread(target=self._patch_engine_token, daemon=True).start()

        self.engine.start()
        self.running = True

        self.start_btn.configure(state="disabled", text="▶  RUNNING")
        self.stop_btn.configure(state="normal")
        mode_txt = "● PAPER" if paper else "● LIVE"
        mode_col = C_YELLOW if paper else C_RED
        self.mode_lbl.configure(text=mode_txt, text_color=mode_col)
        self.status_lbl.configure(
            text=f"Running  |  TF={self.interval_var.get()}min  |  "
                 f"{len(self.instruments)} instruments  |  "
                 f"{'PAPER' if paper else 'LIVE'} mode",
            text_color=C_GREEN,
        )
        self.tabs.set("📊  Live Strategy")

    def _patch_engine_token(self):
        """Fetch token again and patch into engine (needed after _on_resolved)."""
        try:
            client_id, access_token = fetch_token_from_generator()
            self.engine.client_id    = client_id
            self.engine.access_token = access_token
            if self.engine._ws_client:
                self.engine._ws_client.client_id    = client_id
                self.engine._ws_client.access_token = access_token
        except Exception:
            pass

    def _on_start_error(self, err: str):
        self._append_log(f"[ERROR] {err}")
        self.start_btn.configure(state="normal", text="▶  START STRATEGY")
        self.status_lbl.configure(text=f"Error: {err[:80]}", text_color=C_RED)

    # ─────────────────────────────────────────────────────────
    # GUI UPDATE LOOP  (every 2 seconds)
    # ─────────────────────────────────────────────────────────

    def _gui_tick(self):
        try:
            # Clock
            self.clock_lbl.configure(
                text=datetime.now().strftime("🕐  %H:%M:%S   %d %b %Y")
            )

            if self.running and self.engine:
                # Drain engine log
                for line in self.engine.get_logs(20):
                    # Only show new lines (simple dedup by checking log_box)
                    self._append_log(line)
                # Clear engine log after draining so we don't repeat
                with self.engine.lock:
                    self.engine._log_lines.clear()

                # P&L
                total_pnl = sum(s.unrealized_pnl for s in self.instruments)
                pnl_color = C_GREEN if total_pnl >= 0 else C_RED
                self.pnl_lbl.configure(
                    text=f"Net P&L:  ₹{total_pnl:+.2f}", text_color=pnl_color,
                )

                # WS status
                ws_st = self.engine.ws_status
                if "subscribed" in ws_st.lower() or "connected" in ws_st.lower():
                    ws_txt = f"WS ✓  {self.engine.ws_ticks} ticks"
                    ws_col = C_GREEN
                elif "error" in ws_st.lower() or "closed" in ws_st.lower():
                    ws_txt = f"WS ✗  {ws_st[:20]}"
                    ws_col = C_RED
                else:
                    ws_txt = f"WS …  {ws_st[:20]}"
                    ws_col = C_YELLOW
                self.ws_lbl.configure(text=ws_txt, text_color=ws_col)

                # Summary bar
                longs  = sum(1 for s in self.instruments if s.position == "LONG")
                shorts = sum(1 for s in self.instruments if s.position == "SHORT")
                self.summary_lbl.configure(
                    text=f"{longs} LONG   {shorts} SHORT"
                         f"   Next poll: {self.engine.next_poll_at}"
                         f"   REST poll + WS live LTP",
                    text_color=C_GRAY,
                )

                # Update each row
                nq = self.nse_qty_var.get()
                gl = self.gold_lots_var.get()
                sl = self.silv_lots_var.get()
                for row in self._rows:
                    row.update(nq, gl, sl)

        except Exception as e:
            pass
        finally:
            self.after(2000, self._gui_tick)


# ─────────────────────────────────────────────────────────────────────────────
#  TOKEN LOADER
# ─────────────────────────────────────────────────────────────────────────────

TOKEN_SERVER_URL  = "http://localhost:5555/token"
TOKEN_SHARED_FILE = r"C:\balfund_shared\dhan_token.json"


def fetch_token_from_generator() -> tuple:
    # Method 1: HTTP
    try:
        resp = requests.get(TOKEN_SERVER_URL, timeout=3)
        if resp.status_code == 200:
            d = resp.json()
            c, t = d.get("client_id","").strip(), d.get("access_token","").strip()
            if c and t:
                return c, t
    except Exception:
        pass

    # Method 2: Shared file
    try:
        with open(TOKEN_SHARED_FILE, encoding="utf-8") as f:
            d = json.load(f)
        c, t = d.get("client_id","").strip(), d.get("access_token","").strip()
        if c and t:
            return c, t
    except Exception:
        pass

    # Method 3: .env fallback
    try:
        from dhan_token_manager import load_config
        cfg = load_config()
        c   = cfg.get("client_id","").strip()
        t   = cfg.get("access_token","").strip()
        if c and t:
            return c, t
    except Exception:
        pass

    raise RuntimeError(
        "Token not found!\n\n"
        "Please open token_generator.exe and click Generate Token first."
    )


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    app = HATradingApp()
    app.mainloop()


if __name__ == "__main__":
    main()
