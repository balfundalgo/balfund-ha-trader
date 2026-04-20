from __future__ import annotations

import sys as _sys
import os as _os
if getattr(_sys, "frozen", False):
    _sys.path.insert(0, _sys._MEIPASS)

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

import csv
import io
import json
import os
import sys
import time
import threading
import requests
import websocket   # pip install websocket-client

# dhan_ws_client inlined below — no external file needed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# ─────────────────────────────────────────────────────────────────────────────
#  GUI LOG CALLBACK  (replaces terminal print in background threads)
# ─────────────────────────────────────────────────────────────────────────────
_gui_log_callback = None   # set by HATradingApp after construction

def _gui_log(msg: str):
    if _gui_log_callback:
        try:
            _gui_log_callback(msg)
        except Exception:
            pass

# Stub helpers used by resolve functions (no ANSI in GUI build)
def green(s):  return s
def red(s):    return s
def yellow(s): return s
def cyan(s):   return s
def bold(s):   return s
def grey(s):   return s


# ─────────────────────────────────────────────────────────────────────────────
#  DHAN WS CLIENT  (inlined from dhan_ws_client.py — no external file needed)
# ─────────────────────────────────────────────────────────────────────────────

import json
import struct
import threading
import time
from typing import Any, Callable, Dict, Optional

import websocket


REQ_SUB_TICKER = 15
RESP_TICKER = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50

EXCH_SEG_MAP_NUM_TO_NAME = {
    0: "IDX_I",
    1: "NSE_EQ",
    2: "NSE_FNO",
    3: "NSE_CURRENCY",
    4: "BSE_EQ",
    5: "MCX_COMM",
    7: "BSE_CURRENCY",
    8: "BSE_FNO",
}


def _normalize_dhan_epoch(ts: int) -> int:
    """
    Dhan WS sometimes sends ltt with an offset effect in some environments.
    If timestamp appears ahead by ~5.5h, normalize it.
    """
    ts = int(ts)
    now_ts = int(time.time())
    diff = ts - now_ts
    if int(4.5 * 3600) <= diff <= int(6.5 * 3600):
        ts -= 19800
    return ts


def parse_header_8(msg: bytes) -> Optional[Dict[str, Any]]:
    if len(msg) < 8:
        return None

    resp_code = msg[0]
    msg_len = struct.unpack_from("<H", msg, 1)[0]
    exch_seg_num = msg[3]
    sec_id_i = struct.unpack_from("<I", msg, 4)[0]

    return {
        "resp_code": int(resp_code),
        "msg_len": int(msg_len),
        "exch_seg_num": int(exch_seg_num),
        "exch_seg_name": EXCH_SEG_MAP_NUM_TO_NAME.get(int(exch_seg_num), str(exch_seg_num)),
        "security_id": str(sec_id_i),
        "payload": msg[8:],
    }


def parse_ticker(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None

    ltp = struct.unpack_from("<f", payload, 0)[0]
    ltt = struct.unpack_from("<I", payload, 4)[0]

    return {
        "ltp": float(ltp),
        "ltt_epoch": _normalize_dhan_epoch(int(ltt)),
    }


def parse_prev_close(payload: bytes) -> Optional[Dict[str, Any]]:
    if len(payload) < 8:
        return None

    prev_close = struct.unpack_from("<f", payload, 0)[0]
    prev_oi = struct.unpack_from("<I", payload, 4)[0]

    return {
        "prev_close": float(prev_close),
        "prev_oi": int(prev_oi),
    }


class DhanWSClient:
    def __init__(
        self,
        client_id: str,
        access_token: str,
        exchange_segment: str,
        security_id: str,
        on_tick: Callable[[float, int], None],
        on_status: Optional[Callable[[str], None]] = None,
    ) -> None:
        self.client_id = str(client_id).strip()
        self.access_token = str(access_token).strip()
        self.exchange_segment = str(exchange_segment).strip()
        self.security_id = str(security_id).strip()

        self.on_tick_callback = on_tick
        self.on_status_callback = on_status

        self.ws: Optional[websocket.WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()

        self.last_prev_close: Optional[float] = None
        self.last_ticker_key: Optional[tuple] = None
        self.last_packet_time: Optional[float] = None
        self.last_connect_time: Optional[float] = None

        self.packet_counts: Dict[Any, int] = {
            RESP_TICKER: 0,
            RESP_PREV_CLOSE: 0,
            RESP_DISCONNECT: 0,
            "other": 0,
        }

    @property
    def ws_url(self) -> str:
        return (
            f"wss://api-feed.dhan.co?version=2"
            f"&token={self.access_token}"
            f"&clientId={self.client_id}"
            f"&authType=2"
        )

    def _status(self, msg: str) -> None:
        if self.on_status_callback:
            try:
                self.on_status_callback(msg)
            except Exception:
                pass

    def _on_open(self, ws) -> None:
        self.last_connect_time = time.time()
        self._status("Connecting websocket...")

        sub_msg = {
            "RequestCode": REQ_SUB_TICKER,
            "InstrumentCount": 1,
            "InstrumentList": [
                {
                    "ExchangeSegment": self.exchange_segment,
                    "SecurityId": self.security_id,
                }
            ],
        }

        ws.send(json.dumps(sub_msg))
        self._status(f"Subscribed to {self.exchange_segment}:{self.security_id}")

    def _on_message(self, ws, message) -> None:
        try:
            if isinstance(message, str):
                return

            msg = bytes(message)
            hdr = parse_header_8(msg)
            if not hdr:
                return

            code = int(hdr["resp_code"])
            sec = str(hdr["security_id"])

            if sec != self.security_id:
                return

            self.last_packet_time = time.time()

            if code == RESP_TICKER:
                t = parse_ticker(hdr["payload"])
                if not t:
                    return

                ltp = float(t["ltp"])
                ltt_epoch = int(t["ltt_epoch"])

                # prevent exact duplicate packet double counting
                k = (round(ltp, 8), ltt_epoch)
                if self.last_ticker_key == k:
                    return
                self.last_ticker_key = k

                self.packet_counts[RESP_TICKER] = self.packet_counts.get(RESP_TICKER, 0) + 1
                self.on_tick_callback(ltp, ltt_epoch)
                return

            if code == RESP_PREV_CLOSE:
                p = parse_prev_close(hdr["payload"])
                if p:
                    self.last_prev_close = float(p["prev_close"])
                    self.packet_counts[RESP_PREV_CLOSE] = self.packet_counts.get(RESP_PREV_CLOSE, 0) + 1
                return

            if code == RESP_DISCONNECT:
                self.packet_counts[RESP_DISCONNECT] = self.packet_counts.get(RESP_DISCONNECT, 0) + 1
                self._status("Feed disconnect packet received")
                return

            self.packet_counts["other"] = self.packet_counts.get("other", 0) + 1

        except Exception as e:
            self._status(f"WS parse error: {e}")

    def _on_error(self, ws, error) -> None:
        self._status(f"WebSocket error: {error}")

    def _on_close(self, ws, close_status_code, close_msg) -> None:
        self._status(f"WebSocket closed: code={close_status_code}, msg={close_msg}")

    def _run_forever(self) -> None:
        websocket.enableTrace(False)

        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self._status(f"WS exception: {e}")

            if not self.stop_event.is_set():
                time.sleep(2)

    def start(self) -> None:
        self.stop_event.clear()
        self.ws_thread = threading.Thread(target=self._run_forever, daemon=True)
        self.ws_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
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
LOOKBACK_DAYS     = 5

# NIFTY options constants
NIFTY_SPOT_SID        = "13"       # Nifty 50 index security ID on Dhan
NIFTY_SPOT_SEG        = "IDX_I"    # Index segment
NIFTY_LOT_SIZE        = 65         # Fallback lot size (Jan 2026) — auto-read from API
NIFTY_STRIKE_STEP     = 50         # NIFTY strike interval

# Dhan Option Chain API endpoints (used instead of master CSV for lot size + SID)
OPTIONCHAIN_EXPIRY_URL = "https://api.dhan.co/v2/optionchain/expirylist"
OPTIONCHAIN_URL        = "https://api.dhan.co/v2/optionchain"


def first_candle_close_time(interval: str, is_mcx: bool = False) -> str:
    """
    Return HH:MM of when the first candle of the session closes.
    NSE: session starts 09:15, so:
        1min  → 09:16
        5min  → 09:20
        15min → 09:30
    MCX: session starts 09:00, so:
        1min  → 09:01
        5min  → 09:05
        15min → 09:15
    """
    if is_mcx:
        start_h, start_m = 9, 0
    else:
        start_h, start_m = 9, 15

    mins = int(interval)
    total = start_h * 60 + start_m + mins
    return f"{total // 60:02d}:{total % 60:02d}"


def today_has_first_candle_closed(interval: str, is_mcx: bool = False) -> bool:
    """Return True if the first candle of today's session has already closed."""
    close_time = first_candle_close_time(interval, is_mcx)
    return hhmm() >= close_time

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
    max_retries: int = 3,
) -> Dict[str, Any]:
    """
    Place market order with retry on 401 (token refresh) and 429 (rate limit).
    Retries up to max_retries times with exponential backoff.
    """
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
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.post(
                ORDER_URL,
                headers=build_headers(client_id, access_token),
                json=payload,
                timeout=15,
            )
            # 429 rate limit — wait and retry
            if resp.status_code == 429:
                wait = 2 ** attempt   # 2s, 4s, 8s
                time.sleep(wait)
                last_exc = Exception(f"429 Rate Limited (attempt {attempt})")
                continue
            # 401 unauthorized — try refreshing token from generator
            if resp.status_code == 401:
                if attempt < max_retries:
                    try:
                        new_c, new_t = fetch_token_from_generator()
                        access_token = new_t   # use fresh token for retry
                    except Exception:
                        pass
                    time.sleep(2)
                    last_exc = Exception(f"401 Unauthorized (attempt {attempt})")
                    continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(2 ** attempt)
            continue
        except Exception as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(1)
            continue
    raise last_exc or Exception("Order placement failed after retries")

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
        _gui_log(f"  {green('✓')} Using cached master CSV  ({age_h:.1f}h old)  →  {MASTER_CSV_CACHE}")
        with open(MASTER_CSV_CACHE, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            return [{k.strip(): (v.strip() if isinstance(v, str) else v)
                     for k, v in row.items()} for row in reader]

    _gui_log("  Downloading Dhan instrument master CSV  (~50 MB) — please wait ...")
    resp = requests.get(INSTRUMENT_MASTER_URL, timeout=120, stream=True)
    resp.raise_for_status()

    total      = int(resp.headers.get("content-length", 0))
    content    = b""
    downloaded = 0

    last_milestone = -1
    for chunk in resp.iter_content(chunk_size=131072):
        content    += chunk
        downloaded += len(chunk)
        if total:
            pct = int(downloaded / total * 100)
            milestone = (pct // 10) * 10
            if milestone > last_milestone:
                last_milestone = milestone
                mb = downloaded / 1_048_576
                _gui_log(f"    Downloading... {mb:.1f} MB / {total/1_048_576:.1f} MB  ({milestone}%)")

    MASTER_CSV_CACHE.write_bytes(content)
    _gui_log(f"  {green('✓')} Saved to cache → {MASTER_CSV_CACHE}")

    text   = content.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [{k.strip(): (v.strip() if isinstance(v, str) else v)
             for k, v in row.items()} for row in reader]

# ─────────────────────────────────────────────────────────────────────────────
#  INSTRUMENT RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

def resolve_nse_stocks(rows: List[Dict[str, str]], symbols: List[str]) -> Dict[str, str]:
    """One-pass index build → O(n) total, not O(n × m)."""
    _gui_log("  Building NSE_EQ index ...")
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
            _gui_log(f"    {green('✓')} {sym:<15}  sid={sid}")
        else:
            _gui_log(f"    {yellow('✗')} {sym:<15}  NOT FOUND — will skip")
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

        # ── Parse expiry — primary: extract from trading symbol itself ─────
        # e.g. GOLDTEN-30APR2026-FUT → 30APR2026 → 2026-04-30
        expiry_dt = None

        # Method 1: Parse from trading symbol (e.g. "30APR2026" in GOLDTEN-30APR2026-FUT)
        import re as _re
        sym_match = _re.search(r"(\d{2})([A-Z]{3})(\d{4})", trading_sym)
        if sym_match:
            try:
                expiry_dt = datetime.strptime(
                    f"{sym_match.group(1)}{sym_match.group(2)}{sym_match.group(3)}",
                    "%d%b%Y"
                ).date()
            except Exception:
                pass

        # Method 2: Try CSV expiry fields if symbol parse failed
        if not expiry_dt:
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
        _gui_log(f"    NOT FOUND: {prefix}  (tried: {variants})")
        _gui_log(f"    Sample MCX rows from CSV:")
        for s in mcx_samples:
            _gui_log(f"      {s}")
        return None

    active  = sorted(
        [(e, s, t) for e, s, t in found if e and e >= today],
        key=lambda x: x[0]
    )
    no_exp  = [(e, s, t) for e, s, t in found if not e]
    ordered = active + no_exp

    if not ordered:
        expired = [(e, s, t) for e, s, t in found if e and e < today]
        _gui_log(f"    Only expired futures for {prefix}:")
        for e, s, t in expired[:3]:
            _gui_log(f"      contract={t}  expiry={e}  sid={s}")
        return None

    _gui_log(f"    Found {len(ordered)} active future(s) for {bold(prefix)}:")
    for i, (e, s, t) in enumerate(ordered[:6]):
        exp_str = str(e) if e else "unknown"
        marker  = green(f"  [{i+1}]") if i == 0 else grey(f"  [{i+1}]")
        _gui_log(f"    {marker} contract={cyan(t):<38} expiry={exp_str:<12} sid={s}")

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
    _gui_log(f"    {green(chr(10003))} {prefix:<15} -> sid={sid}  "
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
            # Pass exchange segment so engine can do segment-qualified routing
            exch_seg = hdr.get("exch_seg_name", "")
            self._on_tick_cb(sid, ltp, exch_seg)

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
            try:
                self._ws_client.stop()
            except Exception:
                pass

    def subscribe_instrument(self, security_id: str, exchange_segment: str):
        """Dynamically subscribe a new instrument on the live WebSocket connection."""
        entry = (security_id, exchange_segment)
        if entry not in self.instruments:
            self.instruments.append(entry)
        if self._ws:
            try:
                sub = {
                    "RequestCode":     REQ_SUB_TICKER,
                    "InstrumentCount": 1,
                    "InstrumentList":  [
                        {"ExchangeSegment": exchange_segment, "SecurityId": security_id}
                    ],
                }
                self._ws.send(json.dumps(sub))
                self._status(f"WS subscribed: {exchange_segment}:{security_id}")
            except Exception as e:
                self._status(f"WS subscribe error: {e}")

    # ── WebSocket live LTP ────────────────────────────────────

    def _start_ws(self):
        """Start one WebSocket connection subscribing to all instruments + NIFTY spot."""
        # Key by "segment:security_id" to avoid conflicts (e.g. ABB and NIFTY both have sid=13)
        self._sid_map = {
            f"{st.config.exchange_segment}:{st.config.security_id}": st
            for st in self.instruments
        }
        instr_list    = [
            (st.config.security_id, st.config.exchange_segment)
            for st in self.instruments
        ]
        # Always subscribe NIFTY spot for live LTP (needed for options P&L)
        if (NIFTY_SPOT_SID, NIFTY_SPOT_SEG) not in instr_list:
            instr_list.append((NIFTY_SPOT_SID, NIFTY_SPOT_SEG))
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

    def subscribe_nifty_option(self, security_id: str):
        """Subscribe a NIFTY option security_id to live WS feed for LTP updates."""
        if self._ws_client and security_id:
            self._ws_client.subscribe_instrument(security_id, "NSE_FNO")
            # Also add to sid_map so ticks route to nifty_state.opt_ltp
            self._nifty_opt_sid = security_id
            self._log(f"[WS] Subscribed NIFTY option: NSE_FNO:{security_id}")

    def _on_ws_tick(self, security_id: str, ltp: float, exch_seg: str = ""):
        """Called on every live tick — update LTP immediately.
        Uses segment:security_id key to avoid conflicts (e.g. ABB and NIFTY both sid=13).
        """
        # Try segment-qualified key first (most accurate)
        seg_key = f"{exch_seg}:{security_id}" if exch_seg else ""
        st = self._sid_map.get(seg_key) if seg_key else None
        # Fallback to just security_id if segment-key not found
        if st is None:
            st = self._sid_map.get(security_id)
        if st:
            with self.lock:
                st.last_ltp = round(ltp, 2)
            self.ws_ticks += 1
            return
        # Route NIFTY spot tick to nifty_state.spot_ltp
        if security_id == NIFTY_SPOT_SID and self.nifty_state:
            with self.lock:
                self.nifty_state.spot_ltp = round(ltp, 2)
            self.ws_ticks += 1
            return
        # Route NIFTY option tick to nifty_state.opt_ltp
        opt_sid = getattr(self, "_nifty_opt_sid", "")
        if opt_sid and security_id == opt_sid and self.nifty_state:
            with self.lock:
                self.nifty_state.opt_ltp = round(ltp, 2)
            self.ws_ticks += 1


        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
#  STRATEGY ENGINE
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
#  NIFTY OPTIONS STATE  (special instrument — HA on Spot, trade ATM options)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class NiftyOptionsState:
    """Tracks NIFTY ATM options position — HA computed on NIFTY Spot."""
    enabled:       bool  = True
    skip:          bool  = False
    sq_off_done:   bool  = False
    lots:          int   = 1        # number of lots to trade
    lot_size:      int   = 65       # auto-fetched from CSV, updated on first entry
    paper_mode:    bool  = True

    # NIFTY Spot HA data
    spot_ltp:      float = 0.0
    ha_open:       float = 0.0
    ha_close:      float = 0.0
    color:         str   = "-"
    bar_time:      str   = ""
    last_signal:   str   = "-"     # BUY (CE) or SELL (PE)

    # Active option position
    position:      str   = "FLAT"  # FLAT | CE | PE
    opt_sid:       str   = ""      # security_id of active option
    opt_symbol:    str   = ""      # e.g. NIFTY24500CE
    opt_expiry:    str   = ""
    opt_strike:    int   = 0
    entry_price:   float = 0.0
    entry_time:    str   = ""
    opt_ltp:       float = 0.0     # updated via WS

    status:        str   = "Waiting..."
    last_update:   str   = ""

    @property
    def unrealized_pnl(self) -> float:
        if self.position == "FLAT" or not self.entry_price or not self.opt_ltp:
            return 0.0
        return (self.opt_ltp - self.entry_price) * (self.lots * self.lot_size)


def fetch_nifty_expiries(client_id: str, access_token: str) -> List[str]:
    """
    Fetch NIFTY expiry list from Dhan Option Chain API.
    Note: UnderlyingScrip must be int per Dhan API spec.
    Returns list of expiry date strings sorted nearest first.
    """
    payload = {
        "UnderlyingScrip": 13,          # int, not string
        "UnderlyingSeg":   "IDX_I",
    }
    headers = build_headers(client_id, access_token)
    resp    = requests.post(OPTIONCHAIN_EXPIRY_URL, headers=headers,
                            json=payload, timeout=10)
    resp.raise_for_status()
    data     = resp.json()
    expiries = data.get("data", [])
    today    = datetime.now().date()
    valid    = []
    for e in expiries:
        try:
            dt = datetime.strptime(e[:10], "%Y-%m-%d").date()
            if dt >= today:
                valid.append((dt, e))
        except Exception:
            pass
    valid.sort(key=lambda x: x[0])
    return [e for _, e in valid]


def fetch_nifty_atm_option(
    client_id:    str,
    access_token: str,
    spot_price:   float,
    option_type:  str,       # "CE" or "PE"
    expiry:       str,       # expiry string from expiry list e.g. "2026-04-17"
) -> Optional[Dict[str, Any]]:
    """
    Fetch ATM NIFTY option from Dhan Option Chain API.
    API spec: UnderlyingScrip=int, response data.oc keyed by strike float string,
              each value has "ce" and "pe" sub-objects with security_id, last_price.
    Lot size is NOT in the response — read from instrument master CSV or fallback.
    """
    atm_strike = round(spot_price / NIFTY_STRIKE_STEP) * NIFTY_STRIKE_STEP
    payload    = {
        "UnderlyingScrip": 13,          # int per Dhan API spec
        "UnderlyingSeg":   "IDX_I",
        "Expiry":          expiry,
    }
    headers = build_headers(client_id, access_token)
    resp    = requests.post(OPTIONCHAIN_URL, headers=headers,
                            json=payload, timeout=10)
    resp.raise_for_status()
    data  = resp.json()

    # Response: {"data": {"last_price": 23850.0, "oc": {"23850.000000": {"ce":{...},"pe":{...}}}}}
    inner   = data.get("data", {})
    oc_dict = inner.get("oc", {})   # dict keyed by "23850.000000"

    if not oc_dict:
        return None

    # Find ATM strike key — keys are floats like "23850.000000"
    atm_row = None
    for key, val in oc_dict.items():
        try:
            if int(float(key)) == atm_strike:
                atm_row = val
                break
        except Exception:
            pass

    # If exact ATM not found, pick closest strike
    if atm_row is None:
        try:
            keys    = [(abs(int(float(k)) - atm_strike), k, v) for k, v in oc_dict.items()]
            keys.sort(key=lambda x: x[0])
            _, best_key, atm_row = keys[0]
            atm_strike = int(float(best_key))
        except Exception:
            return None

    # Extract CE or PE leg (lowercase keys per API spec)
    opt_key = option_type.lower()   # "ce" or "pe"
    opt_leg = atm_row.get(opt_key)
    if not opt_leg:
        return None

    # security_id, last_price from leg
    sid = str(opt_leg.get("security_id", ""))
    lp  = float(opt_leg.get("last_price", 0) or 0)
    # Build synthetic trading symbol e.g. NIFTY2341724150CE
    exp_str = expiry.replace("-", "")[:8]   # "20260417"
    sym     = f"NIFTY{exp_str}{atm_strike}{option_type}"

    if not sid:
        return None

    return {
        "security_id":    sid,
        "trading_symbol": sym,
        "strike":         atm_strike,
        "expiry":         expiry,
        "lot_size":       NIFTY_LOT_SIZE,   # 65 — API doesn't return lot size
        "ltp":            lp,
    }



class NiftyOptionsEngine:
    """
    Separate engine for NIFTY ATM options trading.
    - Polls NIFTY Spot OHLC → computes HA → generates BUY CE / BUY PE signal
    - On signal change: close existing option position, open new ATM option
    - Uses Dhan Option Chain API to get ATM contract + lot_size (always correct)
    - Runs inside StrategyEngine._run() loop (called from _poll_all)
    """

    def __init__(self, state: NiftyOptionsState, master_rows: List[Dict[str, str]]):
        self.state         = state
        self.master_rows   = master_rows
        self._startup_done = False
        self._expiry_cache: List[str] = []
        self._expiry_cache_ts: float  = 0.0
        self._engine_ref   = None   # set by StrategyEngine.set_nifty_engine()

    def process(
        self,
        client_id:    str,
        access_token: str,
        interval:     str,
        startup:      bool,
        log_fn,        # callable
        lock,
        cached_spot_candles=None,   # pre-fetched from parallel pool
    ):
        st = self.state
        if st.skip:
            log_fn("[NIFTY] Skipped (checkbox OFF)")
            return
        if st.sq_off_done:
            return

        # ── Use cached NIFTY Spot OHLC (fetched in parallel with other instruments) ──
        if cached_spot_candles:
            candles = cached_spot_candles
        else:
            try:
                candles = fetch_ohlc(client_id, access_token,
                                      NIFTY_SPOT_SID, NIFTY_SPOT_SEG, interval)
            except Exception as e:
                with lock:
                    st.status = f"Spot fetch err: {e}"
                return

        if len(candles) < 2:
            with lock:
                st.status = "Waiting for NIFTY data..."
            return

        # ── Wait for first candle of today (same logic as main instruments) ──
        if startup and not self._startup_done:
            first_closed = today_has_first_candle_closed(interval, is_mcx=False)
            if not first_closed:
                close_t = first_candle_close_time(interval, is_mcx=False)
                with lock:
                    st.status = f"Waiting first candle ({close_t})..."
                return
            today_str = datetime.now().strftime("%Y-%m-%d")
            bar_date  = datetime.fromtimestamp(candles[-1]["bucket"]).strftime("%Y-%m-%d")
            if bar_date < today_str:
                with lock:
                    st.status = "Waiting today's NIFTY candle..."
                return

        # ── Compute HA ───────────────────────────────────────────────────────
        ha_candles = compute_ha(candles)
        last_ha    = ha_candles[-1]
        color      = ha_color(last_ha)
        spot       = candles[-1]["close"]
        bar_ts     = datetime.fromtimestamp(last_ha["bucket"]).strftime("%H:%M")

        with lock:
            st.spot_ltp   = round(spot, 2)
            st.ha_open    = round(last_ha["open"],  2)
            st.ha_close   = round(last_ha["close"], 2)
            st.color      = color
            st.bar_time   = bar_ts
            st.last_update = now_str()

        if color == "DOJI":
            with lock:
                st.status = "DOJI — holding"
            return

        # BUY CE on green, BUY PE on red
        signal     = "BUY" if color == "GREEN" else "SELL"
        opt_type   = "CE" if color == "GREEN" else "PE"
        atm_strike = round(spot / NIFTY_STRIKE_STEP) * NIFTY_STRIKE_STEP

        with lock:
            st.last_signal = signal

        # Startup entry
        if startup and not self._startup_done:
            self._startup_done = True
            log_fn(f"[NIFTY] STARTUP → {opt_type} ATM {atm_strike} (HA={color})")
            self._enter_option(client_id, access_token, opt_type,
                               atm_strike, spot, log_fn, lock)
            return
        if startup:
            return

        # Running: act only on color change
        current_pos = st.position
        if (color == "GREEN" and current_pos != "CE") or            (color == "RED"   and current_pos != "PE"):
            log_fn(f"[NIFTY] HA turned {color} at {bar_ts} → {opt_type} ATM {atm_strike}")
            if current_pos != "FLAT":
                self._exit_option(client_id, access_token, "Reversal", log_fn, lock)
                time.sleep(0.5)
            self._enter_option(client_id, access_token, opt_type,
                               atm_strike, spot, log_fn, lock)

    def _enter_option(self, client_id, access_token, opt_type,
                      atm_strike, spot, log_fn, lock):
        st = self.state
        # Resolve ATM contract from master CSV
        # ── Fetch expiry list (cached for 5 min) ─────────────────────────────
        now_ts = time.time()
        if not self._expiry_cache or (now_ts - self._expiry_cache_ts) > 300:
            try:
                self._expiry_cache    = fetch_nifty_expiries(client_id, access_token)
                self._expiry_cache_ts = now_ts
                log_fn(f"[NIFTY] Expiries fetched: {self._expiry_cache[:3]}")
            except Exception as e:
                log_fn(f"[NIFTY] Expiry fetch failed: {e}")
                with lock:
                    st.status = "Expiry fetch failed"
                return

        if not self._expiry_cache:
            with lock:
                st.status = "No NIFTY expiries found"
            return

        nearest_expiry = self._expiry_cache[0]

        # ── Fetch ATM option from Option Chain API ────────────────────────────
        match = None
        try:
            match = fetch_nifty_atm_option(
                client_id, access_token, spot, opt_type, nearest_expiry)
        except Exception as e:
            log_fn(f"[NIFTY] Option chain API failed: {e}")

        if not match:
            log_fn(f"[NIFTY] Could not find ATM {opt_type} {atm_strike} — skipping")
            with lock:
                st.status = f"No contract: ATM {atm_strike} {opt_type}"
            return

        sid      = match["security_id"]
        sym      = match["trading_symbol"]
        lot_size = match.get("lot_size", NIFTY_LOT_SIZE)   # from Option Chain API
        qty      = st.lots * lot_size
        fill     = match.get("ltp", 0.0) or st.opt_ltp or 0.0

        log_fn(f"[NIFTY] Entering {opt_type} {sym} qty={qty} "
               f"(lots={st.lots} × lot_size={lot_size} from API) strike={atm_strike}")

        if st.paper_mode:
            with lock:
                st.position    = opt_type
                st.opt_sid     = sid
                st.opt_symbol  = sym
                st.opt_strike  = atm_strike
                st.opt_expiry  = match["expiry"]
                st.lot_size    = lot_size
                st.entry_price = fill
                st.entry_time  = now_str()
                st.status      = f"[P] {opt_type} {sym} lot={lot_size}"
            # Subscribe option to WS for live LTP
            if self._engine_ref:
                self._engine_ref.subscribe_nifty_option(sid)
            return

        try:
            resp     = place_market_order(
                client_id, access_token,
                sid, "NSE_FNO", "BUY", qty, "INTRADAY", max_retries=3,
            )
            order_id = str(resp.get("orderId") or resp.get("order_id") or "")
            log_fn(f"[NIFTY] ORDER {sym} BUY qty={qty} → id={order_id}")
            if order_id:
                time.sleep(1.5)
                fill = fetch_fill_price(client_id, access_token, order_id, 0.0)
            with lock:
                st.position    = opt_type
                st.opt_sid     = sid
                st.opt_symbol  = sym
                st.opt_strike  = atm_strike
                st.opt_expiry  = match["expiry"]
                st.lot_size    = lot_size
                st.entry_price = fill
                st.entry_time  = now_str()
                st.status      = f"Entered {opt_type} {sym} @{fill:.2f} lot={lot_size}"
            # Subscribe option to WS for live LTP
            if self._engine_ref:
                self._engine_ref.subscribe_nifty_option(sid)
        except Exception as e:
            body = getattr(getattr(e, "response", None), "text", "")
            log_fn(f"[NIFTY] ORDER ERR: {e} | {body[:150]}")
            with lock:
                st.status = f"OrderErr: {str(e)[:40]}"

    def _exit_option(self, client_id, access_token, reason, log_fn, lock):
        st = self.state
        if st.position == "FLAT":
            return
        qty = st.lots * NIFTY_LOT_SIZE
        log_fn(f"[NIFTY] Closing {st.opt_symbol} ({reason})")

        if st.paper_mode:
            pnl = st.unrealized_pnl
            log_fn(f"[NIFTY] [PAPER] CLOSE {st.opt_symbol}  P&L=₹{pnl:+.2f}")
            with lock:
                st.position    = "FLAT"
                st.opt_sid     = ""
                st.opt_symbol  = ""
                st.entry_price = 0.0
                st.status      = f"[P] Closed ({reason})"
            return

        try:
            resp     = place_market_order(
                client_id, access_token,
                st.opt_sid, "NSE_FNO", "SELL", qty, "INTRADAY", max_retries=3,
            )
            order_id = str(resp.get("orderId") or resp.get("order_id") or "")
            log_fn(f"[NIFTY] CLOSE {st.opt_symbol} SELL qty={qty} → id={order_id}")
            with lock:
                st.position    = "FLAT"
                st.opt_sid     = ""
                st.opt_symbol  = ""
                st.entry_price = 0.0
                st.status      = f"Closed ({reason})"
        except Exception as e:
            body = getattr(getattr(e, "response", None), "text", "")
            log_fn(f"[NIFTY] CLOSE ERR: {e} | {body[:150]}")
            with lock:
                st.status = f"CloseErr"

    def manual_squareoff(self, client_id, access_token, log_fn, lock):
        self._exit_option(client_id, access_token, "Manual", log_fn, lock)
        with lock:
            self.state.sq_off_done = True

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
        # NIFTY options — set after init via set_nifty_engine()
        self.nifty_state:  Optional[NiftyOptionsState]  = None
        self.nifty_engine: Optional[NiftyOptionsEngine] = None
        self._nifty_opt_sid: str = ""   # security_id of active option being tracked
        # Shared rate gate for ALL OHLC fetches across all polls
        # Dhan Data API: 5 req/sec → 200ms minimum gap enforced here
        import threading as _th
        self._ohlc_gate = _th.Lock()

    def start(self):
        self._stop.clear()
        threading.Thread(target=self._run, daemon=True).start()
        self._start_ws()

    def stop(self):
        self._stop.set()
        if self._ws_client:
            try:
                self._ws_client.stop()
            except Exception:
                pass
    # ── WebSocket live LTP ────────────────────────────────────

    def _start_ws(self):
        """Start one WebSocket connection subscribing to all instruments + NIFTY spot."""
        # Key by "segment:security_id" to avoid conflicts (e.g. ABB and NIFTY both have sid=13)
        self._sid_map = {
            f"{st.config.exchange_segment}:{st.config.security_id}": st
            for st in self.instruments
        }
        instr_list    = [
            (st.config.security_id, st.config.exchange_segment)
            for st in self.instruments
        ]
        # Always subscribe NIFTY spot for live LTP (needed for options P&L)
        if (NIFTY_SPOT_SID, NIFTY_SPOT_SEG) not in instr_list:
            instr_list.append((NIFTY_SPOT_SID, NIFTY_SPOT_SEG))
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

    def subscribe_nifty_option(self, security_id: str):
        """Subscribe a NIFTY option security_id to live WS feed for LTP updates."""
        if self._ws_client and security_id:
            self._ws_client.subscribe_instrument(security_id, "NSE_FNO")
            # Also add to sid_map so ticks route to nifty_state.opt_ltp
            self._nifty_opt_sid = security_id
            self._log(f"[WS] Subscribed NIFTY option: NSE_FNO:{security_id}")

    def _on_ws_tick(self, security_id: str, ltp: float, exch_seg: str = ""):
        """Called on every live tick — update LTP immediately.
        Uses segment:security_id key to avoid conflicts (e.g. ABB and NIFTY both sid=13).
        """
        # Try segment-qualified key first (most accurate)
        seg_key = f"{exch_seg}:{security_id}" if exch_seg else ""
        st = self._sid_map.get(seg_key) if seg_key else None
        # Fallback to just security_id if segment-key not found
        if st is None:
            st = self._sid_map.get(security_id)
        if st:
            with self.lock:
                st.last_ltp = round(ltp, 2)
            self.ws_ticks += 1
            return
        # Route NIFTY spot tick to nifty_state.spot_ltp
        if security_id == NIFTY_SPOT_SID and self.nifty_state:
            with self.lock:
                self.nifty_state.spot_ltp = round(ltp, 2)
            self.ws_ticks += 1
            return
        # Route NIFTY option tick to nifty_state.opt_ltp
        opt_sid = getattr(self, "_nifty_opt_sid", "")
        if opt_sid and security_id == opt_sid and self.nifty_state:
            with self.lock:
                self.nifty_state.opt_ltp = round(ltp, 2)
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

    def set_nifty_engine(self, state: NiftyOptionsState, master_rows: List[Dict[str, str]]):
        """Attach NIFTY options engine to this strategy engine."""
        self.nifty_state  = state
        self.nifty_engine = NiftyOptionsEngine(state, master_rows)
        self.nifty_engine._engine_ref = self   # back-reference for WS subscription
        state.paper_mode  = self.paper_mode
        # Add NIFTY spot to WS subscription
        if self._ws_client and self._ws_client._stop.is_set() is False:
            pass  # WS already running — NIFTY spot will be subscribed on next start

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
        # NIFTY options — sq-off at NSE time
        if self.nifty_state and not self.nifty_state.sq_off_done:
            if t >= self.nse_sq_time and self.nifty_state.position != "FLAT":
                self._log("[NIFTY] AUTO SQ-OFF")
                if self.nifty_engine:
                    self.nifty_engine.manual_squareoff(
                        self.client_id, self.access_token, self._log, self.lock)
                with self.lock:
                    self.nifty_state.sq_off_done = True

    def _in_session(self, cfg: InstrumentConfig) -> bool:
        t = hhmm()
        return (MCX_SESSION_START <= t < MCX_SESSION_END) if cfg.is_mcx \
               else (NSE_SESSION_START <= t < "15:31")

    # ── Poll ──────────────────────────────────────────────────

    def _poll_all(self, startup: bool = False):
        """
        Optimized poll:
        - OHLC fetches run in parallel (thread pool) — Dhan allows 20 req/sec for data
        - Orders fired sequentially with 100ms gap — Dhan allows 10 orders/sec
        - Total time: ~2-3s for 23 instruments instead of 11.5s sequential
        """
        import concurrent.futures

        # ── Step 1: Fetch OHLC for all instruments in parallel ────────────────
        active = [
            st for st in self.instruments
            if not st.skip and not st.sq_off_done and self._in_session(st.config)
        ]
        # Include a special marker for NIFTY spot if NIFTY engine is active
        _nifty_candles_cache: List[Dict] = []   # populated during parallel fetch
        # Mark closed-market instruments
        for st in self.instruments:
            if not self._in_session(st.config):
                with self.lock:
                    st.status = "Mkt Closed"

        if not active:
            return

        # Fetch OHLC for all active instruments concurrently (max 8 threads)
        signals: Dict[str, Optional[str]] = {}   # name → "BUY"/"SELL"/None

        def _fetch_one(st: InstrumentState):
            try:
                candles = fetch_ohlc(
                    self.client_id, self.access_token,
                    st.config.security_id, st.config.exchange_segment, self.interval,
                )
                if len(candles) < 2:
                    with self.lock:
                        t = hhmm()
                        if st.config.is_mcx:
                            in_s = MCX_SESSION_START <= t < MCX_SESSION_END
                        else:
                            in_s = NSE_SESSION_START <= t < "15:31"
                        st.status = ("Mkt Closed" if not in_s
                                     else "No data from Dhan" if len(candles) == 0
                                     else "Waiting for 2nd bar...")
                    return st.config.name, None

                ha      = compute_ha(candles)
                last_ha = ha[-1]
                color   = ha_color(last_ha)
                ltp     = candles[-1]["close"]
                bar_ts  = datetime.fromtimestamp(last_ha["bucket"]).strftime("%H:%M")

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
                    return st.config.name, None

                signal = "BUY" if color == "GREEN" else "SELL"
                with self.lock:
                    st.last_signal = signal
                return st.config.name, signal

            except Exception as e:
                self._log(f"FETCH ERR {st.config.name}: {e}")
                with self.lock:
                    st.status = f"FetchErr: {str(e)[:30]}"
                return st.config.name, None

        # Sequential gate: lock is held during the 200ms sleep
        # Use class-level shared gate — persists across ALL polls.
        # Dhan Data API: 5 req/sec → 200ms minimum gap between requests.
        _gate = self._ohlc_gate   # ← shared, never reset
        GAP = 0.20

        def _fetch_gated(st):
            with _gate:
                time.sleep(GAP)
            return _fetch_one(st)

        def _fetch_gated_raw(fn):
            with _gate:
                time.sleep(GAP)
            return fn()

        # Fetch NIFTY spot OHLC alongside other instruments if engine is active
        _nifty_spot_candles: list = []

        def _fetch_nifty_spot():
            try:
                c = fetch_ohlc(self.client_id, self.access_token,
                               NIFTY_SPOT_SID, NIFTY_SPOT_SEG, self.interval)
                _nifty_spot_candles.extend(c)
            except Exception as e:
                self._log(f"[NIFTY SPOT FETCH ERR] {e}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            futures = {pool.submit(_fetch_gated, st): st for st in active}
            # Submit NIFTY spot fetch into same pool (uses same rate gate)
            nifty_fut = None
            if self.nifty_engine and self.nifty_state and not self.nifty_state.sq_off_done and not self.nifty_state.skip:
                nifty_fut = pool.submit(_fetch_gated_raw, _fetch_nifty_spot)
            for fut in concurrent.futures.as_completed(futures):
                if self._stop.is_set():
                    break
                try:
                    name, sig = fut.result()
                    signals[name] = sig
                except Exception:
                    pass

        if self._stop.is_set():
            return

        # ── Step 2: Fire orders sequentially (100ms gap = safe at 10/sec) ────
        ORDER_GAP = 0.12   # 120ms between orders → ~8/sec (safe margin under 10/sec)

        for st in active:
            if self._stop.is_set():
                break
            signal = signals.get(st.config.name)
            if signal is None:
                continue

            key = st.config.name

            # Startup: wait for first candle then enter
            if startup and not self._startup_done.get(key):
                first_closed = today_has_first_candle_closed(self.interval, st.config.is_mcx)
                if not first_closed:
                    close_t = first_candle_close_time(self.interval, st.config.is_mcx)
                    with self.lock:
                        st.status = f"Waiting first candle ({close_t})..."
                    continue
                today_str = datetime.now().strftime("%Y-%m-%d")
                bar_date  = datetime.fromtimestamp(
                    int(time.time() // (int(self.interval)*60) * (int(self.interval)*60))
                ).strftime("%Y-%m-%d")
                # Use last candle bucket from ha_open (already stored)
                self._startup_done[key] = True
                self._log(f"STARTUP {key} → {signal}")
                if st.position == "FLAT":
                    self._open_position(st, signal)
                elif (signal == "BUY" and st.position != "LONG") or                      (signal == "SELL" and st.position != "SHORT"):
                    self._reverse_position(st, signal)
                time.sleep(ORDER_GAP)
                continue

            if startup:
                continue

            # Running: act on direction change
            if signal == "BUY" and st.position != "LONG":
                self._log(f"SIGNAL {key} GREEN → BUY")
                if st.position == "FLAT":
                    self._open_position(st, signal)
                else:
                    self._reverse_position(st, signal)
                time.sleep(ORDER_GAP)
            elif signal == "SELL" and st.position != "SHORT":
                self._log(f"SIGNAL {key} RED → SELL")
                if st.position == "FLAT":
                    self._open_position(st, signal)
                else:
                    self._reverse_position(st, signal)
                time.sleep(ORDER_GAP)
            else:
                with self.lock:
                    arrow = "↑LONG" if st.position == "LONG" else "↓SHORT"
                    st.status = f"Holding {arrow}"

        # ── Step 3: NIFTY options (uses pre-fetched spot candles) ────────────
        if self.nifty_engine and self.nifty_state and not self.nifty_state.sq_off_done:
            try:
                # Wait for nifty spot fetch to complete (already in flight)
                if nifty_fut:
                    try: nifty_fut.result(timeout=10)
                    except Exception: pass
                self.nifty_engine.process(
                    self.client_id, self.access_token,
                    self.interval, startup,
                    log_fn=self._log,
                    lock=self.lock,
                    cached_spot_candles=_nifty_spot_candles if _nifty_spot_candles else None,
                )
            except Exception as e:
                self._log(f"[NIFTY ENGINE ERROR] {e}")

    def _process(self, st: InstrumentState, startup: bool):
        candles = fetch_ohlc(
            self.client_id, self.access_token,
            st.config.security_id, st.config.exchange_segment, self.interval,
        )
        if len(candles) < 2:
            with self.lock:
                t = hhmm()
                if st.config.is_mcx:
                    in_session = MCX_SESSION_START <= t < MCX_SESSION_END
                else:
                    in_session = NSE_SESSION_START <= t < "15:31"
                if not in_session:
                    st.status = "Mkt Closed"
                elif len(candles) == 0:
                    st.status = "No data from Dhan"
                else:
                    st.status = "Waiting for 2nd bar..."
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

        # Startup: wait for first candle of TODAY to close before entering
        if startup and not self._startup_done.get(key):
            first_closed = today_has_first_candle_closed(self.interval, st.config.is_mcx)
            if not first_closed:
                close_t = first_candle_close_time(self.interval, st.config.is_mcx)
                with self.lock:
                    st.status = f"Waiting first candle ({close_t})..."
                # Do NOT mark startup_done — retry on next poll
                return

            # Check if last candle is from TODAY (not yesterday)
            today_str = datetime.now().strftime("%Y-%m-%d")
            bar_date  = datetime.fromtimestamp(last_ha["bucket"]).strftime("%Y-%m-%d")
            if bar_date < today_str:
                with self.lock:
                    st.status = "Waiting today's candle..."
                return

            self._startup_done[key] = True
            self._log(f"STARTUP {key} → {signal} (HA={color} bar={bar_ts})")
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
                max_retries=3,
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
            status_code = getattr(getattr(e, "response", None), "status_code", 0)
            if status_code == 401:
                self._log(f"ORDER ERR {st.config.name}: 401 Unauthorized — regenerate token in Token Generator")
                with self.lock:
                    st.status = "401 — Regen token!"
            elif status_code == 429:
                self._log(f"ORDER ERR {st.config.name}: 429 Rate Limited — will retry next candle")
                with self.lock:
                    st.status = "429 Rate Limit"
            else:
                self._log(f"ORDER ERR {st.config.name} {side}: {e} | {body[:150]}")
                with self.lock:
                    st.status = f"OrderErr: {body[:35]}" if body else "OrderErr"

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
                max_retries=3,
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
            status_code = getattr(getattr(e, "response", None), "status_code", 0)
            if status_code == 401:
                self._log(f"CLOSE ERR {st.config.name}: 401 — regenerate token!")
                with self.lock:
                    st.status = "401 — Regen token!"
            elif status_code == 429:
                self._log(f"CLOSE ERR {st.config.name}: 429 Rate Limited")
                with self.lock:
                    st.status = "429 Rate Limit"
            else:
                self._log(f"CLOSE ERR {st.config.name}: {e} | {body[:150]}")
                with self.lock:
                    st.status = "CloseErr"

    def _reverse_position(self, st: InstrumentState, new_dir: str):
        self._close_position(st, "Reversal")
        time.sleep(0.5)
        self._open_position(st, new_dir)



# ─────────────────────────────────────────────────────────────────────────────
#  TOKEN LOADER
# ─────────────────────────────────────────────────────────────────────────────
TOKEN_SERVER_URL  = "http://localhost:5555/token"
TOKEN_SHARED_FILE = r"C:\balfund_shared\dhan_token.json"

def fetch_token_from_generator() -> tuple:
    try:
        resp = requests.get(TOKEN_SERVER_URL, timeout=3)
        if resp.status_code == 200:
            d = resp.json()
            c = d.get("client_id","").strip()
            t = d.get("access_token","").strip()
            if c and t: return c, t
    except Exception: pass
    try:
        with open(TOKEN_SHARED_FILE, encoding="utf-8") as f:
            d = json.load(f)
        c = d.get("client_id","").strip()
        t = d.get("access_token","").strip()
        if c and t: return c, t
    except Exception: pass
    try:
        from dhan_token_manager import load_config
        cfg = load_config()
        c = cfg.get("client_id","").strip()
        t = cfg.get("access_token","").strip()
        if c and t: return c, t
    except Exception: pass
    raise RuntimeError(
        "Token not found!\n\nPlease open Balfund_Token_Generator.exe and click Generate Token."
    )


# ─────────────────────────────────────────────────────────────────────────────
#  GUI APPLICATION
# ─────────────────────────────────────────────────────────────────────────────
import sys
import customtkinter as ctk

if sys.stdout is None:
    sys.stdout = open(os.devnull, "w")
if sys.stderr is None:
    sys.stderr = open(os.devnull, "w")

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")

C_GREEN="#1db954"; C_RED="#e05252"; C_YELLOW="#f0a500"; C_BLUE="#4a9eff"
C_GRAY="#888888";  C_BG="#0f0f1a"; C_FRAME="#16213e";  C_ROW_A="#131a2e"
C_ROW_B="#0f1520"; C_HEADER="#1a2540"; C_SEL="#1a3a5c"

COLS=[
    ("",32),("#",32),("Symbol / Contract",185),("Exch",50),
    ("HA Open",88),("HA Close",88),("Color",68),("Signal",62),
    ("Position",72),("Entry",90),("Qty",44),("LTP",90),
    ("P&L",100),("Bar",50),("Status",175),
]
CI_CHK=0;CI_IDX=1;CI_SYM=2;CI_EXCH=3;CI_HAO=4;CI_HAC=5
CI_COL=6;CI_SIG=7;CI_POS=8;CI_ENT=9;CI_QTY=10;CI_LTP=11
CI_PNL=12;CI_BAR=13;CI_STA=14

class InstrumentRow:
    def __init__(self,parent,idx,st,bg,on_check,on_click):
        self.st=st; self._bg=bg; self._idx=idx-1
        self.frame=ctk.CTkFrame(parent,fg_color=bg,height=30,corner_radius=3)
        self.frame.pack(fill="x",padx=2,pady=1)
        self.frame.pack_propagate(False)
        self.frame.bind("<Button-1>",lambda e,i=self._idx:on_click(i))
        self._labels=[]; x=4
        for ci,(_,w) in enumerate(COLS):
            if ci==CI_CHK:
                self._chk_var=ctk.BooleanVar(value=not st.skip)
                cb=ctk.CTkCheckBox(self.frame,text="",variable=self._chk_var,
                    width=w,checkbox_width=16,checkbox_height=16,
                    command=lambda s=st:on_check(s,self._chk_var.get()))
                cb.place(x=x,rely=0.5,anchor="w"); self._labels.append(cb)
            else:
                lbl=ctk.CTkLabel(self.frame,text="-",width=w,
                    anchor="w" if ci==CI_SYM else "center",
                    font=ctk.CTkFont(size=11),text_color=C_GRAY)
                lbl.place(x=x,rely=0.5,anchor="w")
                lbl.bind("<Button-1>",lambda e,i=self._idx:on_click(i))
                self._labels.append(lbl)
            x+=w+2
        self._labels[CI_IDX].configure(text=str(idx),text_color=C_GRAY)
        sym_txt=st.config.trading_symbol if (st.config.is_mcx and st.config.trading_symbol) else st.config.name
        self._labels[CI_SYM].configure(text=sym_txt,text_color="white",
            font=ctk.CTkFont(size=10,weight="bold"))
        self._labels[CI_EXCH].configure(
            text="MCX" if st.config.is_mcx else "NSE",
            text_color=C_YELLOW if st.config.is_mcx else C_BLUE)

    def set_selected(self,sel):
        self.frame.configure(fg_color=C_SEL if sel else self._bg)

    def update(self,nse_qty,gold_lots,silv_lots,crude_lots=1,zinc_lots=1):
        st=self.st
        def lbl(ci,txt,clr="white"):
            self._labels[ci].configure(text=str(txt),text_color=clr)
        if st.skip:
            for i in range(CI_HAO,CI_STA+1):
                self._labels[i].configure(text="--",text_color=C_GRAY)
            lbl(CI_STA,"Skipped",C_GRAY); return
        lbl(CI_HAO,f"{st.ha_open:.2f}" if st.ha_open else "-")
        lbl(CI_HAC,f"{st.ha_close:.2f}" if st.ha_close else "-")
        lbl(CI_COL,st.color if st.color!="-" else "-",
            {"GREEN":C_GREEN,"RED":C_RED,"DOJI":C_YELLOW}.get(st.color,C_GRAY))
        lbl(CI_SIG,st.last_signal,{"BUY":C_GREEN,"SELL":C_RED}.get(st.last_signal,C_GRAY))
        lbl(CI_POS,st.position,{"LONG":C_GREEN,"SHORT":C_RED,"FLAT":C_GRAY}.get(st.position,C_GRAY))
        lbl(CI_ENT,f"{st.entry_price:.2f}" if st.entry_price else "-")
        mcx_lots = {"GOLDTEN":gold_lots,"SILVERMICRO":silv_lots,
                    "CRUDEOILM":crude_lots,"ZINCMINI":zinc_lots}.get(st.config.name,1)
        qty=(str(st.user_qty) if st.position!="FLAT"
             else str(mcx_lots if st.config.is_mcx else nse_qty))
        lbl(CI_QTY,qty,C_GRAY)
        lbl(CI_LTP,f"{st.last_ltp:.2f}" if st.last_ltp else "-")
        pnl=st.unrealized_pnl
        lbl(CI_PNL,"-" if st.position=="FLAT" else f"Rs {pnl:+.2f}",
            C_GRAY if st.position=="FLAT" else (C_GREEN if pnl>=0 else C_RED))
        lbl(CI_BAR,st.bar_time or "-",C_GRAY)
        s=st.status[:35]
        sc=(C_RED if "Err" in s else C_GREEN if "LONG" in s
            else C_RED if "SHORT" in s else C_YELLOW if "Closed" in s else C_GRAY)
        lbl(CI_STA,s,sc)


class HATradingApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund  HA + SMA(1)  Strategy Trader  v1.0")
        self.geometry("1760x930"); self.minsize(1400,700)
        self.configure(fg_color=C_BG)
        global _gui_log_callback; _gui_log_callback=self._append_log
        self.engine=None; self.instruments=[]; self.running=False
        self._rows=[]; self._selected_idx=None
        self._client_id=""; self._access_token=""
        self.interval_var=ctk.StringVar(value="5")
        self.nse_qty_var=ctk.IntVar(value=10)
        self.gold_lots_var=ctk.IntVar(value=1)
        self.silv_lots_var=ctk.IntVar(value=1)
        self.crude_lots_var=ctk.IntVar(value=1)
        self.zinc_lots_var=ctk.IntVar(value=1)
        self.nse_sq_var=ctk.StringVar(value="15:15")
        self.mcx_sq_var=ctk.StringVar(value="23:25")
        self.paper_var=ctk.BooleanVar(value=True)
        self.nifty_opt_var=ctk.BooleanVar(value=False)   # enable NIFTY ATM options
        self.nifty_lots_var=ctk.IntVar(value=1)           # number of NIFTY lots
        self.nifty_state: Optional[NiftyOptionsState] = None
        self._master_rows: List[Dict[str, str]] = []
        self._build_ui(); self.after(2000,self._gui_tick)
        self.after(500, self._preload_instruments)  # load table on startup

    def _preload_instruments(self):
        """Resolve instruments in background on startup so table is pre-populated."""
        self._append_log("[AUTO] Pre-loading instruments in background...")
        threading.Thread(target=self._do_preload, daemon=True).start()

    def _do_preload(self):
        try:
            c, t = fetch_token_from_generator()
            self._client_id = c; self._access_token = t
            rows = load_master_csv()
            nse_ids = resolve_nse_stocks(rows, NSE_STOCKS)
            instruments = []
            nq = self.nse_qty_var.get()
            for sym in NSE_STOCKS:
                sid = nse_ids.get(sym)
                if not sid: continue
                instruments.append(InstrumentState(
                    config=InstrumentConfig(name=sym, exchange_segment="NSE_EQ",
                        security_id=sid, product_type="INTRADAY", lot_multiplier=1),
                    api_qty=nq))
            for sym, lv in [("GOLDTEN", self.gold_lots_var), ("SILVERMICRO", self.silv_lots_var),
                             ("CRUDEOILM", self.crude_lots_var), ("ZINCMINI", self.zinc_lots_var)]:
                m = resolve_mcx_future(rows, sym, allow_pick=False)
                if not m: continue
                mult = MCX_LOT_MULTIPLIERS[sym]
                instruments.append(InstrumentState(
                    config=InstrumentConfig(name=sym, exchange_segment="MCX_COMM",
                        security_id=m["security_id"], product_type="INTRADAY",
                        lot_multiplier=mult, trading_symbol=m["trading_symbol"],
                        expiry=m["expiry"]),
                    api_qty=lv.get() * mult))
            if instruments:
                self.instruments = instruments
                # Create placeholder NIFTY state for display
                if not self.nifty_state:
                    self.nifty_state = NiftyOptionsState(
                        lots=self.nifty_lots_var.get(), skip=not self.nifty_opt_var.get())
                self.after(0, self._build_instrument_rows)
                self.after(0, lambda: self._append_log(
                    f"[AUTO] {len(instruments)} instruments pre-loaded — ready to START."))
        except Exception as e:
            self.after(0, lambda err=str(e): self._append_log(f"[AUTO PRE-LOAD] {err}"))

    def _build_ui(self):
        top=ctk.CTkFrame(self,fg_color=C_HEADER,height=52,corner_radius=0)
        top.pack(fill="x"); top.pack_propagate(False)
        ctk.CTkLabel(top,text="  BALFUND   HA + SMA(1)  STRATEGY TRADER",
            font=ctk.CTkFont(size=17,weight="bold"),text_color=C_BLUE).pack(side="left",padx=18)
        self.pnl_lbl=ctk.CTkLabel(top,text="Net P&L:  Rs 0.00",
            font=ctk.CTkFont(size=14,weight="bold"),text_color=C_YELLOW)
        self.pnl_lbl.pack(side="right",padx=18)
        self.clock_lbl=ctk.CTkLabel(top,text="",font=ctk.CTkFont(size=12),text_color=C_GRAY)
        self.clock_lbl.pack(side="right",padx=12)
        self.ws_lbl=ctk.CTkLabel(top,text="WS: --",font=ctk.CTkFont(size=11),text_color=C_GRAY)
        self.ws_lbl.pack(side="right",padx=12)
        self.mode_lbl=ctk.CTkLabel(top,text="PAPER",
            font=ctk.CTkFont(size=12,weight="bold"),text_color=C_YELLOW)
        self.mode_lbl.pack(side="right",padx=12)
        self.tabs=ctk.CTkTabview(self,fg_color=C_BG,
            segmented_button_selected_color=C_BLUE)
        self.tabs.pack(fill="both",expand=True,padx=6,pady=(0,6))
        for t in ["Settings","Live Strategy","Log"]: self.tabs.add(t)
        self._build_settings(self.tabs.tab("Settings"))
        self._build_strategy(self.tabs.tab("Live Strategy"))
        self._build_log(self.tabs.tab("Log"))

    def _build_settings(self,parent):
        parent.configure(fg_color=C_BG)
        parent.grid_columnconfigure((0,1,2,3),weight=1)
        def card(col,title):
            f=ctk.CTkFrame(parent,fg_color=C_FRAME,corner_radius=10)
            f.grid(row=0,column=col,padx=10,pady=12,sticky="nsew")
            ctk.CTkLabel(f,text=title,font=ctk.CTkFont(size=13,weight="bold"),
                text_color=C_BLUE).pack(pady=(14,8)); return f
        tf=card(0,"Candle Timeframe")
        for v,l in [("1","1 Minute"),("5","5 Minutes"),("15","15 Minutes")]:
            ctk.CTkRadioButton(tf,text=l,variable=self.interval_var,
                value=v,font=ctk.CTkFont(size=12)).pack(anchor="w",padx=22,pady=5)
        qf=card(1,"Quantity per Trade")
        for l,v,u in [("NSE Stocks:",self.nse_qty_var,"shares"),
                      ("GOLDTEN:",self.gold_lots_var,"lots"),
                      ("SILVERMICRO:",self.silv_lots_var,"lots"),
                      ("CRUDEOILM:",self.crude_lots_var,"lots"),
                      ("ZINCMINI:",self.zinc_lots_var,"lots")]:
            r=ctk.CTkFrame(qf,fg_color="transparent"); r.pack(fill="x",padx=16,pady=5)
            ctk.CTkLabel(r,text=l,width=130,anchor="w",font=ctk.CTkFont(size=12)).pack(side="left")
            ctk.CTkEntry(r,textvariable=v,width=70,font=ctk.CTkFont(size=12)).pack(side="left",padx=6)
            ctk.CTkLabel(r,text=u,text_color=C_GRAY,font=ctk.CTkFont(size=11)).pack(side="left")
        # NIFTY options row
        ctk.CTkFrame(qf,fg_color="#333333",height=1).pack(fill="x",padx=16,pady=4)
        nr=ctk.CTkFrame(qf,fg_color="transparent"); nr.pack(fill="x",padx=16,pady=4)
        ctk.CTkCheckBox(nr,text="NIFTY ATM Options",variable=self.nifty_opt_var,
            font=ctk.CTkFont(size=12,weight="bold"),
            text_color=C_YELLOW).pack(side="left")
        ctk.CTkEntry(nr,textvariable=self.nifty_lots_var,width=50,
            font=ctk.CTkFont(size=12)).pack(side="left",padx=8)
        ctk.CTkLabel(nr,text="lots (lot size auto-fetched from API)",text_color=C_GRAY,
            font=ctk.CTkFont(size=11)).pack(side="left")
        sf=card(2,"Auto Square-off")
        for l,v in [("NSE (HH:MM):",self.nse_sq_var),("MCX (HH:MM):",self.mcx_sq_var)]:
            r=ctk.CTkFrame(sf,fg_color="transparent"); r.pack(fill="x",padx=16,pady=8)
            ctk.CTkLabel(r,text=l,width=120,anchor="w",font=ctk.CTkFont(size=12)).pack(side="left")
            ctk.CTkEntry(r,textvariable=v,width=90,font=ctk.CTkFont(size=13)).pack(side="left",padx=6)
        ctk.CTkLabel(sf,text="NSE: 09:15 to 15:30\nMCX: 09:00 to 23:30",
            text_color=C_GRAY,font=ctk.CTkFont(size=11),justify="left").pack(padx=16,pady=6,anchor="w")
        mf=card(3,"Trading Mode")
        ctk.CTkSwitch(mf,text="Paper Mode (safe)",variable=self.paper_var,
            font=ctk.CTkFont(size=12),onvalue=True,offvalue=False,
            progress_color=C_YELLOW).pack(padx=22,pady=4,anchor="w")
        ctk.CTkLabel(mf,text="Turn OFF for live orders",text_color=C_RED,
            font=ctk.CTkFont(size=11)).pack(padx=22,pady=4,anchor="w")
        # START/STOP only on Live Strategy tab — hidden stubs here for reference
        self.start_btn=ctk.CTkButton(parent,text="",width=1,height=1,
            fg_color=C_BG,hover_color=C_BG,border_width=0,
            command=self._on_start)
        self.stop_btn=ctk.CTkButton(parent,text="",width=1,height=1,
            fg_color=C_BG,hover_color=C_BG,border_width=0,
            command=self._on_stop,state="disabled")
        self.status_lbl=ctk.CTkLabel(parent,text="")

    def _build_strategy(self,parent):
        parent.configure(fg_color=C_BG)
        hdr=ctk.CTkFrame(parent,fg_color=C_HEADER,height=30,corner_radius=4)
        hdr.pack(fill="x",padx=4,pady=(4,0)); hdr.pack_propagate(False)
        x=4
        for name,w in COLS:
            ctk.CTkLabel(hdr,text=name,width=w,anchor="center",
                font=ctk.CTkFont(size=10,weight="bold"),
                text_color=C_BLUE).place(x=x,rely=0.5,anchor="w"); x+=w+2
        act=ctk.CTkFrame(parent,fg_color=C_FRAME,height=36,corner_radius=4)
        act.pack(fill="x",padx=4,pady=(2,0)); act.pack_propagate(False)
        ctk.CTkLabel(act,text="Selected row:",text_color=C_GRAY,
            font=ctk.CTkFont(size=11)).pack(side="left",padx=10)
        ctk.CTkButton(act,text="Manual Sq.Off",width=130,height=26,
            fg_color=C_RED,hover_color="#c43a3a",font=ctk.CTkFont(size=11),
            command=self._manual_sqoff).pack(side="left",padx=6)
        
        self.selected_lbl=ctk.CTkLabel(act,text="(click a row)",
            text_color=C_GRAY,font=ctk.CTkFont(size=11))
        self.selected_lbl.pack(side="left",padx=10)
        # ── Quick start bar (START/STOP directly on strategy tab) ────────────
        qstart=ctk.CTkFrame(parent,fg_color=C_FRAME,height=42,corner_radius=4)
        qstart.pack(fill="x",padx=4,pady=(2,0)); qstart.pack_propagate(False)
        self.tab_start_btn=ctk.CTkButton(qstart,text="START STRATEGY",
            width=180,height=30,font=ctk.CTkFont(size=12,weight="bold"),
            fg_color=C_GREEN,hover_color="#17a844",command=self._on_start)
        self.tab_start_btn.pack(side="left",padx=8,pady=6)
        self.tab_stop_btn=ctk.CTkButton(qstart,text="STOP",
            width=90,height=30,font=ctk.CTkFont(size=12,weight="bold"),
            fg_color=C_RED,hover_color="#c43a3a",
            command=self._on_stop,state="disabled")
        self.tab_stop_btn.pack(side="left",padx=4,pady=6)
        self.tab_status_lbl=ctk.CTkLabel(qstart,text="Not started",
            font=ctk.CTkFont(size=11),text_color=C_GRAY)
        self.tab_status_lbl.pack(side="left",padx=12)

        self.sum_bar=ctk.CTkFrame(parent,fg_color=C_FRAME,height=28,corner_radius=4)
        self.sum_bar.pack(fill="x",padx=4,pady=(2,0)); self.sum_bar.pack_propagate(False)
        self.sum_lbl=ctk.CTkLabel(self.sum_bar,text="0 LONG  0 SHORT  Next poll: --",
            font=ctk.CTkFont(size=11),text_color=C_GRAY)
        self.sum_lbl.pack(side="left",padx=10)
        self.scroll=ctk.CTkScrollableFrame(parent,fg_color=C_BG,corner_radius=0)
        self.scroll.pack(fill="both",expand=True,padx=4,pady=4)

    def _build_log(self,parent):
        parent.configure(fg_color=C_BG)
        self.log_box=ctk.CTkTextbox(parent,font=ctk.CTkFont(family="Courier",size=11),
            fg_color="#0a0a14",text_color="#cccccc")
        self.log_box.pack(fill="both",expand=True,padx=6,pady=6)
        ctk.CTkButton(parent,text="Clear Log",width=100,
            command=lambda:self.log_box.delete("0.0","end")).pack(side="right",padx=8,pady=4)

    def _append_log(self,msg):
        try: self.log_box.insert("end",str(msg)+"\n"); self.log_box.see("end")
        except Exception: pass

    def _build_instrument_rows(self):
        for w in self.scroll.winfo_children(): w.destroy()
        self._rows.clear(); self._selected_idx=None
        for idx,st in enumerate(self.instruments):
            bg=C_ROW_A if idx%2==0 else C_ROW_B
            row=InstrumentRow(self.scroll,idx+1,st,bg,
                on_check=lambda s,c:setattr(s,"skip",not c),
                on_click=self._select_row)
            self._rows.append(row)
        # Always show NIFTY options row (greyed if disabled)
        if self.nifty_opt_var.get() or self.nifty_state:
            if not self.nifty_state:
                # Create placeholder state so row renders
                self.nifty_state = NiftyOptionsState(
                    lots=self.nifty_lots_var.get(), skip=True)
            self._build_nifty_row(len(self.instruments)+1)

    def _build_nifty_row(self, idx: int):
        """Special row for NIFTY ATM options."""
        bg = C_ROW_A if idx % 2 == 0 else C_ROW_B
        nst = self.nifty_state
        nifty_idx = len(self.instruments)   # index past the normal rows
        frame = ctk.CTkFrame(self.scroll, fg_color=bg, height=30, corner_radius=3)
        frame.pack(fill="x", padx=2, pady=1)
        frame.pack_propagate(False)
        frame.bind("<Button-1>", lambda e: self._select_row(nifty_idx))
        x = 4
        # Checkbox
        chk_var = ctk.BooleanVar(value=not nst.skip)
        cb = ctk.CTkCheckBox(frame, text="", variable=chk_var,
            width=COLS[0][1], checkbox_width=16, checkbox_height=16,
            command=lambda: setattr(nst, "skip", not chk_var.get()))
        cb.place(x=x, rely=0.5, anchor="w"); x += COLS[0][1] + 2
        # Index
        ctk.CTkLabel(frame, text=str(idx), width=COLS[1][1], anchor="center",
            font=ctk.CTkFont(size=11), text_color=C_GRAY).place(x=x, rely=0.5, anchor="w")
        x += COLS[1][1] + 2
        # Symbol — dynamic: shows active contract e.g. "NIFTY24500CE"
        sym_lbl = ctk.CTkLabel(frame, text="NIFTY ATM OPTIONS", width=COLS[2][1], anchor="w",
            font=ctk.CTkFont(size=10, weight="bold"),
            text_color=C_YELLOW)
        sym_lbl.place(x=x, rely=0.5, anchor="w")
        x += COLS[2][1] + 2
        # Exchange
        ctk.CTkLabel(frame, text="NSE", width=COLS[3][1], anchor="center",
            font=ctk.CTkFont(size=11), text_color=C_BLUE).place(x=x, rely=0.5, anchor="w")
        x += COLS[3][1] + 2
        # Dynamic labels: HA Open, HA Close, Color, Signal, Position, Entry, Qty, LTP, P&L, Bar, Status
        self._nifty_labels = {"sym": sym_lbl}   # sym is dynamic
        dyn_names = ["ha_o","ha_c","color","signal","pos","entry","qty","ltp","pnl","bar","status"]
        for ci, name in enumerate(dyn_names, start=4):
            w = COLS[ci][1] if ci < len(COLS) else 90
            lbl = ctk.CTkLabel(frame, text="-", width=w, anchor="center",
                font=ctk.CTkFont(size=11), text_color=C_GRAY)
            lbl.place(x=x, rely=0.5, anchor="w")
            lbl.bind("<Button-1>", lambda e, i=nifty_idx: self._select_row(i))
            self._nifty_labels[name] = lbl
            x += w + 2

    def _select_row(self,idx):
        if self._selected_idx is not None and self._selected_idx<len(self._rows):
            self._rows[self._selected_idx].set_selected(False)
        self._selected_idx=idx
        if idx < len(self._rows):
            self._rows[idx].set_selected(True)
        if idx < len(self.instruments):
            st=self.instruments[idx]
            self.selected_lbl.configure(text=f"  {st.config.name}  ({st.position})",text_color="white")
        else:
            # NIFTY row selected
            nst=self.nifty_state
            pos=nst.position if nst else "FLAT"
            self.selected_lbl.configure(text=f"  NIFTY OPTIONS  ({pos})",text_color="white")

    def _on_start(self):
        self.start_btn.configure(state="disabled",text="Resolving...")
        self.tab_start_btn.configure(state="disabled",text="Resolving...")
        self.status_lbl.configure(text="Resolving instruments...",text_color=C_YELLOW)
        self.tab_status_lbl.configure(text="Resolving instruments...",text_color=C_YELLOW)
        threading.Thread(target=self._resolve_and_start,daemon=True).start()

    def _on_stop(self):
        if self.engine: self.engine.stop()
        self.running=False
        self.start_btn.configure(state="normal",text="START STRATEGY")
        self.stop_btn.configure(state="disabled")
        self.tab_start_btn.configure(state="normal",text="START STRATEGY")
        self.tab_stop_btn.configure(state="disabled")
        self.status_lbl.configure(text="Stopped",text_color=C_GRAY)
        self.tab_status_lbl.configure(text="Stopped",text_color=C_GRAY)
        self._append_log("[INFO] Stopped.")

    def _manual_sqoff(self):
        if self._selected_idx is None or not self.engine: return
        idx = self._selected_idx
        if idx >= len(self.instruments):
            # NIFTY row selected
            if self.engine.nifty_engine:
                threading.Thread(
                    target=self.engine.nifty_engine.manual_squareoff,
                    args=(self.engine.client_id, self.engine.access_token,
                          self.engine._log, self.engine.lock),
                    daemon=True).start()
        else:
            name=self.instruments[idx].config.name
            threading.Thread(target=self.engine.manual_squareoff,
                args=(name,),daemon=True).start()

    def _toggle_skip(self):
        if self._selected_idx is None: return
        st=self.instruments[self._selected_idx]
        st.skip=not st.skip
        self._rows[self._selected_idx]._chk_var.set(not st.skip)
        self.selected_lbl.configure(
            text=f"  {st.config.name}  ({'skipped' if st.skip else 'active'})",
            text_color=C_YELLOW if st.skip else "white")

    def _log_bg(self,msg):
        self.after(0,lambda m=msg:self._append_log(m))

    def _resolve_and_start(self):
        try:
            self._log_bg("[1/4] Fetching token...")
            c,t=fetch_token_from_generator()
            self._client_id=c; self._access_token=t
            self._log_bg(f"      Client: {c}  Token: {t[:20]}...")
            self._log_bg("[2/4] Loading master CSV...")
            rows=load_master_csv()
            self._log_bg(f"      {len(rows):,} rows")
            self._log_bg("[3/4] Resolving NSE stocks...")
            nse_ids=resolve_nse_stocks(rows,NSE_STOCKS)
            instruments=[]
            nq=self.nse_qty_var.get()
            for sym in NSE_STOCKS:
                sid=nse_ids.get(sym)
                if not sid: continue
                instruments.append(InstrumentState(
                    config=InstrumentConfig(name=sym,exchange_segment="NSE_EQ",
                        security_id=sid,product_type="INTRADAY",lot_multiplier=1),
                    api_qty=nq))
            self._log_bg("[4/4] Resolving MCX futures...")
            for sym,lv in [("GOLDTEN",self.gold_lots_var),("SILVERMICRO",self.silv_lots_var),
                           ("CRUDEOILM",self.crude_lots_var),("ZINCMINI",self.zinc_lots_var)]:
                m=resolve_mcx_future(rows,sym,allow_pick=False)
                if not m: self._log_bg(f"      WARNING: {sym} not found"); continue
                mult=MCX_LOT_MULTIPLIERS[sym]
                instruments.append(InstrumentState(
                    config=InstrumentConfig(name=sym,exchange_segment="MCX_COMM",
                        security_id=m["security_id"],product_type="INTRADAY",
                        lot_multiplier=mult,trading_symbol=m["trading_symbol"],
                        expiry=m["expiry"]),
                    api_qty=lv.get()*mult))
            self._log_bg(f"      {len(instruments)} instruments ready.")
            if not instruments:
                self.after(0,lambda:self._on_start_error("No instruments resolved.")); return
            self.instruments=instruments
            self._master_rows=rows  # save for nifty engine
            # Create NIFTY options state based on checkbox
            if self.nifty_opt_var.get():
                self.nifty_state = NiftyOptionsState(
                    lots=self.nifty_lots_var.get(),
                    skip=False,
                    paper_mode=self.paper_var.get())
                self._log_bg(f"[NIFTY] Options enabled — lots={self.nifty_lots_var.get()}")
            else:
                # Keep placeholder for display but mark as skipped
                if self.nifty_state:
                    self.nifty_state.skip = True
            self.after(0,self._on_resolved)
        except Exception as e:
            self.after(0,lambda err=str(e):self._on_start_error(err))

    def _on_resolved(self):
        self._build_instrument_rows()
        paper=self.paper_var.get()
        self.engine=StrategyEngine(
            client_id=self._client_id,access_token=self._access_token,
            instruments=self.instruments,interval=self.interval_var.get(),
            nse_sq_time=self.nse_sq_var.get(),mcx_sq_time=self.mcx_sq_var.get(),
            paper_mode=paper)
        # Wire NIFTY options engine if enabled and not skipped
        if self.nifty_state and not self.nifty_state.skip and self._master_rows:
            self.engine.set_nifty_engine(self.nifty_state, self._master_rows)
            self._log_bg("[NIFTY] Options engine wired — will trade ATM CE/PE")
        elif self.nifty_state and self.nifty_state.skip:
            self._log_bg("[NIFTY] Options checkbox is OFF — not trading")
        self.engine.start(); self.running=True
        self.tabs.set("Live Strategy")
        self.mode_lbl.configure(text="PAPER" if paper else "LIVE",
            text_color=C_YELLOW if paper else C_RED)
        status_txt=f"Running | TF={self.interval_var.get()}min | {len(self.instruments)} instruments | {'PAPER' if paper else 'LIVE'}"
        self.start_btn.configure(state="disabled",text="RUNNING")
        self.stop_btn.configure(state="normal")
        self.tab_start_btn.configure(state="disabled",text="RUNNING")
        self.tab_stop_btn.configure(state="normal")
        self.status_lbl.configure(text=status_txt,text_color=C_GREEN)
        self.tab_status_lbl.configure(text=status_txt,text_color=C_GREEN)

    def _on_start_error(self,err):
        self._append_log(f"[ERROR] {err}")
        self.start_btn.configure(state="normal",text="START STRATEGY")
        self.tab_start_btn.configure(state="normal",text="START STRATEGY")
        self.status_lbl.configure(text=f"Error: {err[:80]}",text_color=C_RED)
        self.tab_status_lbl.configure(text=f"Error: {err[:60]}",text_color=C_RED)

    def _gui_tick(self):
        try:
            self.clock_lbl.configure(text=datetime.now().strftime("  %H:%M:%S  %d %b %Y"))
            if self.running and self.engine:
                logs=self.engine.get_logs(30)
                with self.engine.lock: self.engine._log_lines.clear()
                for line in logs: self._append_log(line)
                total_pnl=sum(s.unrealized_pnl for s in self.instruments)
                self.pnl_lbl.configure(
                    text=f"Net P&L:  Rs {total_pnl:+.2f}",
                    text_color=C_GREEN if total_pnl>=0 else C_RED)
                ws_st=self.engine.ws_status
                if "subscribed" in ws_st.lower() or "connected" in ws_st.lower():
                    self.ws_lbl.configure(text=f"WS OK  {self.engine.ws_ticks} ticks",text_color=C_GREEN)
                elif "error" in ws_st.lower() or "closed" in ws_st.lower():
                    self.ws_lbl.configure(text=f"WS ERR",text_color=C_RED)
                else:
                    self.ws_lbl.configure(text="WS connecting...",text_color=C_YELLOW)
                longs=sum(1 for s in self.instruments if s.position=="LONG")
                shorts=sum(1 for s in self.instruments if s.position=="SHORT")
                active=sum(1 for s in self.instruments if not s.skip)
                self.sum_lbl.configure(
                    text=f"{longs} LONG   {shorts} SHORT   "
                         f"{active}/{len(self.instruments)} active   "
                         f"Next poll: {self.engine.next_poll_at}   REST + WS live LTP")
                nq=self.nse_qty_var.get(); gl=self.gold_lots_var.get(); sl=self.silv_lots_var.get()
                for row in self._rows: row.update(nq,gl,sl,self.crude_lots_var.get(),self.zinc_lots_var.get())
                # Update NIFTY row
                if self.nifty_state and hasattr(self, "_nifty_labels"):
                    self._update_nifty_row()
        except Exception: pass
        finally: self.after(2000,self._gui_tick)



    def _update_nifty_row(self):
        nst = self.nifty_state
        lbl = self._nifty_labels
        def L(k, txt, clr="white"):
            if k in lbl: lbl[k].configure(text=str(txt), text_color=clr)
        # Update symbol to show active contract or base label
        if nst.position != "FLAT" and nst.opt_symbol:
            L("sym", nst.opt_symbol, C_GREEN if nst.position=="CE" else C_RED)
        elif nst.opt_strike:
            L("sym", f"NIFTY ATM {nst.opt_strike}", C_YELLOW)
        else:
            L("sym", "NIFTY ATM OPTIONS", C_YELLOW)
        L("ha_o",  f"{nst.ha_open:.2f}"  if nst.ha_open  else "-")
        L("ha_c",  f"{nst.ha_close:.2f}" if nst.ha_close else "-")
        col_clr = {"GREEN":C_GREEN,"RED":C_RED,"DOJI":C_YELLOW}.get(nst.color, C_GRAY)
        L("color", nst.color if nst.color != "-" else "-", col_clr)
        sig_txt = "BUY CE" if nst.last_signal=="BUY" else ("BUY PE" if nst.last_signal=="SELL" else "-")
        sig_clr = C_GREEN if nst.last_signal=="BUY" else (C_RED if nst.last_signal=="SELL" else C_GRAY)
        L("signal", sig_txt, sig_clr)
        pos_clr = C_GREEN if nst.position=="CE" else (C_RED if nst.position=="PE" else C_GRAY)
        L("pos",   nst.position if nst.position!="FLAT" else "FLAT", pos_clr)
        L("entry", f"{nst.entry_price:.2f}" if nst.entry_price else "-")
        L("qty",   str(nst.lots * nst.lot_size) if nst.position!="FLAT" else f"{self.nifty_lots_var.get()}L×{nst.lot_size}", C_GRAY)
        # LTP column: show option LTP if in position, else show NIFTY spot
        if nst.position != "FLAT" and nst.opt_ltp:
            L("ltp", f"Opt:{nst.opt_ltp:.2f}", "white")
        elif nst.spot_ltp:
            L("ltp", f"N:{nst.spot_ltp:.0f}", C_BLUE)
        else:
            L("ltp", "-", C_GRAY)
        pnl = nst.unrealized_pnl
        L("pnl",   f"Rs{pnl:+.2f}" if nst.position!="FLAT" else "-",
          C_GREEN if pnl>=0 else C_RED)
        L("bar",   nst.bar_time or "-", C_GRAY)
        sc = (C_RED if "Err" in nst.status else C_GREEN if "CE" in nst.position
              else C_RED if "PE" in nst.position else C_GRAY)
        L("status", nst.status[:35], sc)

def main():
    try:
        app = HATradingApp()
        app.mainloop()
    except Exception as e:
        import traceback
        try:
            import tkinter.messagebox as mb
            mb.showerror(
                "Balfund HA Trader — Startup Error",
                f"The application crashed on startup:\n\n{traceback.format_exc()}"
            )
        except Exception:
            pass

if __name__ == "__main__":
    main()
