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
#  TERMINAL DISPLAY
# ─────────────────────────────────────────────────────────────────────────────

# Column widths (plain chars, ANSI codes don't count toward width)
CW = dict(no=3, sym=14, exch=5, ha_o=9, ha_c=9,
          color=7, sig=5, pos=6, entry=10, qty=5, ltp=10, pnl=10, bar=6, status=22)
SEP = "  "

def _hdr() -> str:
    return SEP.join([
        f"{'#':>{CW['no']}}",        f"{'SYMBOL':<{CW['sym']}}",
        f"{'EXCH':<{CW['exch']}}",   f"{'HA OPEN':>{CW['ha_o']}}",
        f"{'HA CLOSE':>{CW['ha_c']}}", f"{'COLOR':<{CW['color']}}",
        f"{'SIG':<{CW['sig']}}",      f"{'POS':<{CW['pos']}}",
        f"{'ENTRY':>{CW['entry']}}",  f"{'QTY':>{CW['qty']}}",
        f"{'LTP':>{CW['ltp']}}",      f"{'P&L':>{CW['pnl']}}",
        f"{'BAR':<{CW['bar']}}",      f"{'STATUS':<{CW['status']}}",
    ])

def _divider(c="─") -> str:
    w = sum(CW.values()) + len(SEP) * (len(CW) - 1)
    return c * w

def _row(idx: int, st: InstrumentState) -> str:
    # Color
    if   st.color == "GREEN": color_s = green(f"{'GREEN':<{CW['color']}}")
    elif st.color == "RED":   color_s = red(f"{'RED':<{CW['color']}}")
    elif st.color == "DOJI":  color_s = yellow(f"{'DOJI':<{CW['color']}}")
    else:                     color_s = grey(f"{'-':<{CW['color']}}")

    # Signal
    if   st.last_signal == "BUY":  sig_s = green(f"{'BUY':<{CW['sig']}}")
    elif st.last_signal == "SELL": sig_s = red(f"{'SELL':<{CW['sig']}}")
    else:                          sig_s = grey(f"{'-':<{CW['sig']}}")

    # Position
    if   st.position == "LONG":  pos_s = green(f"{'LONG':<{CW['pos']}}")
    elif st.position == "SHORT": pos_s = red(f"{'SHORT':<{CW['pos']}}")
    else:                        pos_s = grey(f"{'FLAT':<{CW['pos']}}")

    # P&L
    pnl = st.unrealized_pnl
    if st.position == "FLAT":
        pnl_s = grey(f"{'-':>{CW['pnl']}}")
    elif pnl >= 0:
        pnl_s = green(f"{f'+{pnl:.2f}':>{CW['pnl']}}")
    else:
        pnl_s = red(f"{f'{pnl:.2f}':>{CW['pnl']}}")

    # Exchange
    exch_s = yellow(f"{'MCX':<{CW['exch']}}") if st.config.is_mcx \
             else blue(f"{'NSE':<{CW['exch']}}")

    # Status
    s = st.status[:CW["status"]]
    if "Err" in s:         status_s = red(f"{s:<{CW['status']}}")
    elif "LONG" in s:      status_s = green(f"{s:<{CW['status']}}")
    elif "SHORT" in s:     status_s = red(f"{s:<{CW['status']}}")
    elif "Mkt Closed" in s:status_s = grey(f"{s:<{CW['status']}}")
    elif "Closed" in s:    status_s = yellow(f"{s:<{CW['status']}}")
    else:                  status_s = grey(f"{s:<{CW['status']}}")

    ha_o  = f"{st.ha_open:.2f}"    if st.ha_open    else "-"
    ha_c  = f"{st.ha_close:.2f}"   if st.ha_close   else "-"
    ltp   = f"{st.last_ltp:.2f}"   if st.last_ltp   else "-"
    entry = f"{st.entry_price:.2f}" if st.entry_price else "-"
    qty   = str(st.user_qty)        if st.position != "FLAT" else "-"

    # Show contract month for MCX rows e.g. "GOLDTEN Apr25"
    sym_display = st.config.name
    if st.config.is_mcx and st.config.expiry:
        try:
            from datetime import datetime as _dt
            exp = _dt.strptime(st.config.expiry, "%Y-%m-%d")
            sym_display = f"{st.config.name} {exp.strftime('%b%y')}"
        except Exception:
            sym_display = f"{st.config.name} {st.config.expiry[:7]}"

    return SEP.join([
        f"{idx:>{CW['no']}}",
        f"{sym_display:<{CW['sym']}}",
        exch_s,
        f"{ha_o:>{CW['ha_o']}}",
        f"{ha_c:>{CW['ha_c']}}",
        color_s, sig_s, pos_s,
        f"{entry:>{CW['entry']}}",
        f"{qty:>{CW['qty']}}",
        f"{ltp:>{CW['ltp']}}",
        pnl_s,
        f"{st.bar_time or '-':<{CW['bar']}}",
        status_s,
    ])


def redraw(engine: StrategyEngine, instruments: List[InstrumentState]):
    os.system("cls" if os.name == "nt" else "clear")

    total_pnl = sum(s.unrealized_pnl for s in instruments)
    pnl_s     = green(f"₹{total_pnl:+.2f}") if total_pnl >= 0 else red(f"₹{total_pnl:+.2f}")
    longs     = sum(1 for s in instruments if s.position == "LONG")
    shorts    = sum(1 for s in instruments if s.position == "SHORT")

    print(bold(blue(_divider("═"))))
    print(bold(
        f"  BALFUND  HA+SMA(1)  v1.0"
        f"   TF={engine.interval}min"
        f"   {now_str()}"
        f"   Net P&L: {pnl_s}"
    ))
    # Build MCX contract info line
    mcx_info = "  ".join(
        f"{s.config.name}={cyan(s.config.trading_symbol or '?')} exp={s.config.expiry[:7] if s.config.expiry else '?'}"
        for s in instruments if s.config.is_mcx
    )
    print(
        f"  {green(str(longs))} LONG  {red(str(shorts))} SHORT"
        f"   NSE sq-off: {engine.nse_sq_time}"
        f"   MCX sq-off: {engine.mcx_sq_time}"
        f"   Next poll: {cyan(engine.next_poll_at)}"
        f"   {grey('REST polling')}   {yellow('[PAPER]') if engine.paper_mode else red('[LIVE]')}"
    )
    if mcx_info:
        print(f"  MCX contracts: {mcx_info}")
    print(bold(blue(_divider("═"))))
    print(bold(grey(_hdr())))
    print(grey(_divider()))

    for idx, st in enumerate(instruments, 1):
        print(_row(idx, st))

    print(grey(_divider()))

    # Last 8 log lines
    logs = engine.get_logs(8)
    if logs:
        print()
        for line in logs:
            print(f"  {grey(line)}")

    print()
    print(grey("  Ctrl+C to stop"))


# ─────────────────────────────────────────────────────────────────────────────
#  TOKEN LOADER
#  Priority: 1) localhost:5555/token (token_generator.py HTTP server)
#            2) C:\balfund_shared\dhan_token.json (shared file)
#            3) .env via dhan_token_manager.py (legacy fallback)
# ─────────────────────────────────────────────────────────────────────────────

TOKEN_SERVER_URL  = "http://localhost:5555/token"
TOKEN_SHARED_FILE = r"C:\balfund_shared\dhan_token.json"


def fetch_token_from_generator() -> tuple:
    """
    Fetch client_id + access_token from the running Token Generator app.
    Returns (client_id, access_token). Exits with error if all methods fail.
    """

    # ── Method 1: HTTP localhost:5555 ────────────────────────
    print(f"  Trying HTTP  →  {TOKEN_SERVER_URL}")
    try:
        resp = requests.get(TOKEN_SERVER_URL, timeout=3)
        if resp.status_code == 200:
            data         = resp.json()
            client_id    = data.get("client_id",    "").strip()
            access_token = data.get("access_token", "").strip()
            generated_at = data.get("generated_at", "")
            if client_id and access_token:
                print(f"  {green(chr(10003))} Token received via HTTP")
                print(f"     Client  : {client_id}")
                print(f"     Token   : {access_token[:24]}...")
                print(f"     Generated: {generated_at}")
                return client_id, access_token
            else:
                print(f"  {yellow('?')} HTTP response missing fields: {data}")
    except requests.exceptions.ConnectionError:
        print(f"  {yellow('!')} Token Generator not running on port 5555")
    except Exception as e:
        print(f"  {yellow('!')} HTTP fetch failed: {e}")

    # ── Method 2: Shared JSON file ────────────────────────────
    print(f"  Trying file  →  {TOKEN_SHARED_FILE}")
    try:
        with open(TOKEN_SHARED_FILE, encoding="utf-8") as f:
            data         = json.load(f)
        client_id    = data.get("client_id",    "").strip()
        access_token = data.get("access_token", "").strip()
        generated_at = data.get("generated_at", "")
        if client_id and access_token:
            print(f"  {green(chr(10003))} Token loaded from shared file")
            print(f"     Client  : {client_id}")
            print(f"     Token   : {access_token[:24]}...")
            print(f"     Generated: {generated_at}")
            return client_id, access_token
        else:
            print(f"  {yellow('?')} File missing fields")
    except FileNotFoundError:
        print(f"  {yellow('!')} Shared file not found")
    except Exception as e:
        print(f"  {yellow('!')} File read failed: {e}")

    # ── Method 3: .env via dhan_token_manager (legacy) ───────
    print(f"  Trying .env  →  dhan_token_manager.py")
    try:
        from dhan_token_manager import load_config
        cfg          = load_config()
        client_id    = cfg.get("client_id",    "").strip()
        access_token = cfg.get("access_token", "").strip()
        if client_id and access_token:
            print(f"  {green(chr(10003))} Token loaded from .env")
            print(f"     Client  : {client_id}")
            print(f"     Token   : {access_token[:24]}...")
            return client_id, access_token
    except Exception as e:
        print(f"  {yellow('!')} .env fallback failed: {e}")

    # ── All methods failed ────────────────────────────────────
    print()
    print(red("  ✗  Could not load token from any source!"))
    print(red("     Please open token_generator.py and click Generate Token first."))
    print(red("     Then restart this strategy."))
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    os.system("cls" if os.name == "nt" else "clear")
    print(bold(blue("=" * 65)))
    print(bold("  BALFUND  HA + SMA(1) STRATEGY TRADER  v1.0  [Terminal]"))
    print(bold(blue("=" * 65)))
    print()

    # ── 1. Load token from Token Generator ───────────────────
    print(bold("[ 1/4 ]  Fetching token from Dhan Token Generator ..."))
    client_id, access_token = fetch_token_from_generator()
    print()

    # ── 2. Config prompts ─────────────────────────────────────
    print(bold("[ 2/4 ]  Strategy settings  (Enter = keep default)"))
    interval     = prompt("Timeframe [1 / 5 / 15] min", "5")
    if interval not in ("1", "5", "15"):
        interval = "5"
    nse_qty      = int(prompt("NSE quantity (shares)",     "10") or "10")
    gt_lots      = int(prompt("GOLDTEN lots",              "1")  or "1")
    sm_lots      = int(prompt("SILVERMICRO lots",          "1")  or "1")
    nse_sq       = prompt("NSE auto sq-off time (HH:MM)", "15:15")
    mcx_sq       = prompt("MCX auto sq-off time (HH:MM)", "23:25")
    paper_raw    = prompt("Paper mode? (y=paper / n=live)", "y")
    paper_mode   = paper_raw.strip().lower() not in ("n", "no", "live")
    print()

    # ── 3. Resolve instruments ────────────────────────────────
    print(bold("[ 3/4 ]  Resolving instruments ..."))
    print()
    try:
        rows = load_master_csv()
    except Exception as e:
        print(red(f"  ✗  Failed: {e}"))
        sys.exit(1)
    print()

    instruments: List[InstrumentState] = []

    print(bold("  NSE EQ stocks:"))
    nse_ids = resolve_nse_stocks(rows, NSE_STOCKS)
    for sym in NSE_STOCKS:
        sid = nse_ids.get(sym)
        if not sid:
            continue
        cfg_i = InstrumentConfig(
            name=sym, exchange_segment="NSE_EQ",
            security_id=sid, product_type="INTRADAY", lot_multiplier=1,
        )
        instruments.append(InstrumentState(config=cfg_i, api_qty=nse_qty))

    print()
    print(bold("  MCX Futures:"))
    for sym, user_lots in [("GOLDTEN", gt_lots), ("SILVERMICRO", sm_lots)]:
        match = resolve_mcx_future(rows, sym)
        if not match:
            continue
        mult  = MCX_LOT_MULTIPLIERS[sym]
        cfg_i = InstrumentConfig(
            name=sym, exchange_segment="MCX_COMM",
            security_id=match["security_id"], product_type="INTRADAY",
            lot_multiplier=mult,
            trading_symbol=match["trading_symbol"], expiry=match["expiry"],
        )
        instruments.append(InstrumentState(config=cfg_i, api_qty=user_lots * mult))

    print()
    print(f"  {green('✓')}  {bold(str(len(instruments)))} instruments resolved")
    print()

    if not instruments:
        print(red("  ✗  Nothing resolved — cannot start."))
        sys.exit(1)

    # ── 4. Start engine ───────────────────────────────────────
    print(bold("[ 4/4 ]  Starting strategy ..."))
    mode_label = yellow("📋 PAPER MODE  (no real orders)") if paper_mode else red("⚡ LIVE MODE  (real orders!)")
    print(f"  TF={interval}min   NSE sq-off={nse_sq}   MCX sq-off={mcx_sq}")
    print(f"  Mode: {mode_label}")
    print()

    engine = StrategyEngine(
        client_id=client_id, access_token=access_token,
        instruments=instruments, interval=interval,
        nse_sq_time=nse_sq, mcx_sq_time=mcx_sq,
        paper_mode=paper_mode,
    )
    engine.start()
    print(green("  ▶  Engine running — fetching first bars (~30s for 23 instruments) ..."))
    print(grey("     Table will appear automatically. Ctrl+C to stop.\n"))

    # ── Display loop ──────────────────────────────────────────
    try:
        time.sleep(6)   # brief head-start before first redraw
        while True:
            redraw(engine, instruments)
            time.sleep(2)
    except KeyboardInterrupt:
        print()
        print(yellow("  ⏹  Stopping ..."))
        engine.stop()
        time.sleep(1)
        print(green("  ✓  Done. Goodbye.\n"))


if __name__ == "__main__":
    main()
