"""
╔══════════════════════════════════════════════════════════════════════════╗
║  BALFUND TRADING PVT. LTD.                                               ║
║  Heikin-Ashi + Smoothed MA  Strategy Trader  v2.0                       ║
║                                                                          ║
║  Architecture : WebSocket-first — seed via REST once, then pure WS      ║
║  Instruments  : 173 NSE EQ + 5 MCX Futures + NIFTY ATM Options         ║
║  Timeframes   : 5s / 1m / 5m / 15m  (all WS-driven)                    ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations
import os, sys, time, json, struct, threading, csv, io
from datetime import datetime, timedelta, date
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Tuple
from pathlib import Path
import requests, websocket
import customtkinter as ctk

# ══════════════════════════════════════════════════════════════════════════════
#  PATHS
# ══════════════════════════════════════════════════════════════════════════════
BASE_DIR = Path(sys.executable).parent if getattr(sys,"frozen",False) else Path(__file__).parent
SHARED_TOKEN_FILE = Path(r"C:\balfund_shared\dhan_token.json")

# ══════════════════════════════════════════════════════════════════════════════
#  LIGHT THEME  (same palette as SSL Supporter v3)
# ══════════════════════════════════════════════════════════════════════════════
BG   = "#FFFFFF"           # app background
CARD = "#F0F4FA"           # card / row background
HBG  = "#1E3A5F"           # navy header
AC   = "#1A56DB"           # accent blue
ACL  = "#3B82F6"           # accent light
GOLD = "#D4A017"           # Balfund gold
GR   = "#059669"           # green  (BUY / LONG)
RD   = "#DC2626"           # red    (SELL / SHORT)
GY   = "#9CA3AF"           # muted
TX   = "#111827"           # primary text
TD   = "#6B7280"           # dim text
BD   = "#D1D5DB"           # borders
ROW_A = "#FFFFFF"
ROW_B = "#F8FAFC"
FONT  = ("Segoe UI", 11, "bold")
FONTB = ("Segoe UI", 11, "bold")

# ══════════════════════════════════════════════════════════════════════════════
#  CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
INTRADAY_URL      = "https://api.dhan.co/v2/charts/intraday"
MASTER_CSV_URL    = "https://images.dhan.co/api-data/api-scrip-master.csv"
OC_EXPIRY_URL     = "https://api.dhan.co/v2/optionchain/expirylist"
OC_URL            = "https://api.dhan.co/v2/optionchain"
ORDER_URL         = "https://api.dhan.co/v2/orders"
WS_URL_TMPL       = "wss://api-feed.dhan.co?version=2&token={tok}&clientId={cid}&authType=2"

NIFTY_SID         = "13"
NIFTY_SEG         = "IDX_I"
NIFTY_FNO_SEG     = "NSE_FNO"
NIFTY_STRIKE_STEP = 50

REQ_SUB    = 15
RESP_TICK  = 2
HDR_FMT    = ">H H I I H f"   # 18 bytes

NSE_SESSION = ("09:15", "15:30")
MCX_SESSION = ("09:00", "23:30")

MCX_LOT_MULTIPLIERS = {
    "GOLDTEN":    1,
    "SILVERMICRO":1,
    "CRUDEOILM":  10,
    "ZINCMINI":   1000,
    "GOLDPETAL":  1,
}

NSE_STOCKS = [
    "GMRAIRPORT","IRFC",      "DBREALTY",  "DEVYANI",   "VMM",
    "IREDA",     "WELSPUNLIV","PNB",        "JMFINANCIL","J&KBANK",
    "IEX",       "JSWCEMENT", "MOTHERSON", "RCF",       "NIACL",
    "ADANIPOWER","IRCON",     "CANBK",     "SAMMAANCAP","NCC",
    "GAIL",      "SAIL",      "PPLPHARMA", "CESC",      "BANKINDIA",
    "IGL",       "IOC",       "ITCHOTELS", "CGCL",      "JINDALSAW",
    "SAPPHIRE",  "AWL",       "HUDCO",     "BANDHANBNK","CASTROLIND",
    "FINPIPE",   "UNIONBANK", "ASHOKLEY",  "AEGISVOPAK","MRPL",
    "TATASTEEL", "ENGINERSIN","WIPRO",     "RITES",     "FSL",
    "FIRSTCRY",  "ACMESOLAR", "ANGELONE",  "ETERNAL",   "APTUS",
    "JIOFIN",    "CAMPUS",    "JYOTHYLAB", "SCI",       "NLCINDIA",
    "CROMPTON",  "SONATSOFTW","BLS",       "GPIL",      "CUB",
    "ITI",       "BHEL",      "NYKAA",     "REDINGTON", "MANAPPURAM",
    "JSWINFRA",  "ONGC",      "LTF",       "PCBL",      "AFCONS",
    "FEDERALBNK","GSPL",      "RVNL",      "JWL",       "RAILTEL",
    "TARIL",     "NUVOCO",    "PETRONET",  "HONASA",    "COHANCE",
    "BANKBARODA","SWIGGY",    "POWERGRID", "LATENTVIEW","KARURVYSYA",
    "RBLBANK",   "ITC",       "PRAJIND",   "EXIDEIND",  "EIHOTEL",
    "VGUARD",    "BPCL",      "SAREGAMA",  "IGIL",      "ABCAPITAL",
    "GODIGIT",   "RECLTD",    "TMPV",      "SWANCORP",  "M&MFIN",
    "MANYAVAR",  "GICRE",     "INDIACEM",  "TRIVENI",   "GUJGASLTD",
    "BLUEJET",   "NTPC",      "LTFOODS",   "TATAPOWER", "RHIM",
    "BSOFT",     "HINDPETRO", "NATIONALUM","FIVESTAR",  "KALYANKJIL",
    "SUMICHEM",  "KOTAKBANK", "BIOCON",    "HAPPSTMNDS","ELECON",
    "SYNGENE",   "PFC",       "USHAMART",  "POONAWALLA","DELHIVERY",
    "AARTIIND",  "THELEELA",  "CHAMBLFERT","APOLLOTYRE","VBL",
    "BERGEPAINT","HEXT",      "COALINDIA", "EMAMILTD",  "HSCL",
    "JKTYRE",    "INDUSTOWER","AGARWALEYE","TEJASNET",  "STARHEALTH",
    "INDGN",     "BEL",       "AMBUJACEM", "NEWGEN",    "TRITURBINE",
    "CONCOR",    "ATGL",      "OIL",       "DABUR",     "AADHARHFC",
    "ANANTRAJ",  "JUBLFOOD",  "AIIL",      "JSWENERGY", "IIFL",
    "PATANJALI", "AKUMS",     "BALRAMCHIN","MINDACORP", "SONACOMS",
    "LICHSGFIN", "JBMA",      "HEG",       "ELGIEQUIP", "KEC",
    "VTL",       "GMDCLTD",   "MAHSEAMLES","IRCTC",     "SARDAEN",
    "RKFORGE",   "ZENSARTECH","JUBLINGREA",
]

MCX_SYMBOLS = ["GOLDTEN","SILVERMICRO","CRUDEOILM","ZINCMINI","GOLDPETAL"]

# ══════════════════════════════════════════════════════════════════════════════
#  DATA CLASSES
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class InstrCfg:
    name:   str
    seg:    str
    sid:    str
    is_mcx: bool = False
    lot_mult: int = 1
    trading_symbol: str = ""
    expiry: str = ""

@dataclass
class InstrState:
    cfg:         InstrCfg
    qty:         int   = 1
    position:    str   = "FLAT"
    entry:       float = 0.0
    ltp:         float = 0.0
    ha_open:     float = 0.0
    ha_close:    float = 0.0
    color:       str   = "-"
    signal:      str   = ""
    status:      str   = "Waiting..."
    bar_time:    str   = "-"
    skip:        bool  = False
    sq_off_done: bool  = False
    last_tick:   float = 0.0

    @property
    def unrealised_pnl(self) -> float:
        if self.position == "FLAT" or self.entry == 0: return 0.0
        d = (self.ltp - self.entry) if self.position == "LONG" else (self.entry - self.ltp)
        return round(d * self.qty * self.cfg.lot_mult, 2)

# ══════════════════════════════════════════════════════════════════════════════
#  CANDLE AGGREGATOR  (WS-driven candles)
# ══════════════════════════════════════════════════════════════════════════════
class CandleAgg:
    def __init__(self, interval_sec: int, seed: Optional[List[dict]] = None):
        self.iv    = interval_sec
        self._lock = threading.Lock()
        self._closed: List[dict] = list(seed or [])
        self._cur:    Optional[dict] = None

    def on_tick(self, price: float, ts: float):
        bucket = int(ts // self.iv) * self.iv
        with self._lock:
            if self._cur is None or self._cur["bucket"] != bucket:
                if self._cur:
                    self._closed.append(self._cur)
                    if len(self._closed) > 500: self._closed = self._closed[-500:]
                self._cur = {"bucket": bucket, "open": price, "high": price,
                             "low": price, "close": price}
            else:
                c = self._cur
                if price > c["high"]: c["high"] = price
                if price < c["low"]:  c["low"]  = price
                c["close"] = price

    def candles(self, include_current=False) -> List[dict]:
        with self._lock:
            r = list(self._closed)
            if include_current and self._cur:
                r.append(dict(self._cur))
            return r

    def ready(self, n=2) -> bool:
        with self._lock: return len(self._closed) >= n

# ══════════════════════════════════════════════════════════════════════════════
#  HA + SMA
# ══════════════════════════════════════════════════════════════════════════════
def compute_ha(candles: List[dict], sma: int = 1) -> List[dict]:
    ha: List[dict] = []
    for i, c in enumerate(candles):
        o, h, l, cl = float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"])
        hac = (o + h + l + cl) / 4
        hao = (ha[-1]["open"] + ha[-1]["close"]) / 2 if i else (o + cl) / 2
        ha.append({"bucket": c["bucket"],
                   "open": hao, "high": max(h, hao, hac),
                   "low": min(l, hao, hac), "close": hac})
    if sma <= 1: return ha
    p = sma
    sha = []
    for i, b in enumerate(ha):
        if i < p - 1:
            sha.append(dict(b))
        else:
            sc = sum(ha[j]["close"] for j in range(i-p+1, i+1)) / p
            so = sum(ha[j]["open"]  for j in range(i-p+1, i+1)) / p
            sha.append({"bucket": b["bucket"], "open": so,
                        "high": max(b["high"], so, sc),
                        "low": min(b["low"], so, sc), "close": sc})
    return sha

def ha_color(c: dict) -> str:
    if c["close"] > c["open"]: return "GREEN"
    if c["close"] < c["open"]: return "RED"
    return "DOJI"

# ══════════════════════════════════════════════════════════════════════════════
#  TOKEN LOADER
# ══════════════════════════════════════════════════════════════════════════════
def load_token() -> Tuple[str, str]:
    try:
        if SHARED_TOKEN_FILE.exists():
            d = json.loads(SHARED_TOKEN_FILE.read_text())
            return d.get("client_id",""), d.get("access_token","")
    except: pass
    try:
        r = requests.get("http://localhost:5555/token", timeout=3)
        d = r.json()
        return d.get("client_id",""), d.get("access_token","")
    except: pass
    return os.getenv("DHAN_CLIENT_ID",""), os.getenv("DHAN_ACCESS_TOKEN","")

# ══════════════════════════════════════════════════════════════════════════════
#  REST — OHLC FETCH (seed only)
# ══════════════════════════════════════════════════════════════════════════════
def fetch_ohlc(client_id: str, token: str, sid: str, seg: str, iv: str,
               days: int = 5) -> List[dict]:
    now = datetime.now()
    hdrs = {"Content-Type":"application/json","access-token":token,"client-id":client_id}
    pl = {"securityId": sid, "exchangeSegment": seg, "instrument": "INDEX" if seg=="IDX_I" else "FUTIDX" if "FUT" in seg else "EQUITY",
          "expiryCode": 0, "oi": False, "interval": iv,
          "fromDate": (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),
          "toDate": now.strftime("%Y-%m-%d %H:%M:%S")}
    for attempt in range(3):
        r = requests.post(INTRADAY_URL, headers=hdrs, json=pl, timeout=20)
        if r.status_code == 429:
            time.sleep(2 * (attempt + 1)); continue
        r.raise_for_status(); break
    d = r.json()
    opens, highs, lows, closes = d.get("open",[]), d.get("high",[]), d.get("low",[]), d.get("close",[])
    times = d.get("timestamp", d.get("start_Time", []))
    out = []
    for i in range(len(opens)):
        ts = times[i]
        if isinstance(ts, str):
            try: epoch = int(datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").timestamp())
            except: epoch = 0
        else: epoch = int(ts)
        out.append({"bucket": epoch, "open": float(opens[i]), "high": float(highs[i]),
                    "low": float(lows[i]), "close": float(closes[i])})
    return out

# ══════════════════════════════════════════════════════════════════════════════
#  REST — MASTER CSV (instrument resolution)
# ══════════════════════════════════════════════════════════════════════════════
MASTER_CACHE = BASE_DIR / "dhan_master_cache.csv"

def load_master(log_fn=print) -> List[dict]:
    cache_age = 0.0
    if MASTER_CACHE.exists():
        cache_age = (time.time() - MASTER_CACHE.stat().st_mtime) / 3600
        if cache_age < 12:
            log_fn(f"  ✓ Using cached master CSV ({cache_age:.1f}h old)")
            raw = MASTER_CACHE.read_text(encoding="utf-8", errors="ignore")
            if raw.startswith("﻿"): raw = raw[1:]
            return list(csv.DictReader(io.StringIO(raw)))
    log_fn("  Downloading Dhan master CSV...")
    r = requests.get(MASTER_CSV_URL, stream=True, timeout=120)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    buf = io.BytesIO()
    done = 0
    last_pct = -1
    for chunk in r.iter_content(65536):
        buf.write(chunk); done += len(chunk)
        if total:
            pct = done * 100 // total
            milestone = (pct // 10) * 10
            if milestone > last_pct:
                log_fn(f"    {done/1e6:.1f} MB / {total/1e6:.1f} MB  ({milestone}%)")
                last_pct = milestone
    # Force UTF-8 decode + strip BOM (prevents NSE section being missed)
    raw = buf.getvalue().decode("utf-8", errors="ignore")
    if raw.startswith("﻿"): raw = raw[1:]
    MASTER_CACHE.write_text(raw, encoding="utf-8")
    return list(csv.DictReader(io.StringIO(raw)))

def _build_nse_index(rows: List[dict]) -> Dict[str, str]:
    """Build {TICKER → sid} using SEM_TRADING_SYMBOL (e.g. INFY-EQ → INFY)."""
    import re as _re
    idx = {}
    for row in rows:
        exch   = row.get("SEM_EXM_EXCH_ID","").strip().upper()
        series = row.get("SEM_SERIES","").strip().upper()
        ts     = row.get("SEM_TRADING_SYMBOL","").strip().upper()
        sid    = row.get("SEM_SMST_SECURITY_ID","").strip()
        if exch != "NSE": continue
        if series not in ("EQ","BE","BZ","IL","SM"): continue
        if not ts or not sid: continue
        ticker = _re.sub(r"-(EQ|BE|BZ|IL|SM)$", "", ts).strip()
        if ticker and series == "EQ":
            idx[ticker] = sid
        elif ticker and ticker not in idx:
            idx[ticker] = sid
    return idx

def resolve_nse(rows: List[dict], sym: str) -> Optional[str]:
    # Legacy single-lookup (slow) — use _build_nse_index for bulk
    idx = _build_nse_index(rows)
    return idx.get(sym.upper())

def resolve_mcx(rows: List[dict], sym: str, log_fn=print) -> Optional[dict]:
    import re
    today = date.today()
    safe  = today + timedelta(days=2)
    prefix_variants = [sym.upper(),
                       sym.upper().replace("MICRO","MIC"),
                       sym.upper().replace("MINI","")]
    found = []
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID","").strip().upper() != "MCX": continue
        ts = row.get("SEM_TRADING_SYMBOL","").strip().upper()
        sn = row.get("SEM_CUSTOM_SYMBOL","").strip().upper()
        if not any(ts.startswith(v) for v in prefix_variants): continue
        if not ts.endswith("-FUT"): continue
        m = re.search(r"(\d{2})([A-Z]{3})(\d{4})", ts)
        if not m: continue
        MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
                  "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
        try:
            exp = date(int(m.group(3)), MONTHS[m.group(2)], int(m.group(1)))
        except: continue
        sid = row.get("SEM_SMST_SECURITY_ID","").strip()
        lu  = int(float(row.get("SEM_LOT_UNITS","1") or 1))
        found.append((exp, sid, ts, lu))
    active = sorted([x for x in found if x[0] >= safe], key=lambda x: x[0])
    if not active:
        active = sorted([x for x in found if x[0] >= today], key=lambda x: x[0])
    if not active: return None
    e, sid, ts, lu = active[0]
    log_fn(f"    ✓ {sym:<12} sid={sid}  {ts}  expiry={e}  lot_units={lu}")
    return {"sid": sid, "trading_symbol": ts, "expiry": str(e), "lot_units": lu}

# ══════════════════════════════════════════════════════════════════════════════
#  ORDER API
# ══════════════════════════════════════════════════════════════════════════════
def place_order(client_id: str, token: str, seg: str, sid: str,
                side: str, qty: int, paper: bool, log_fn=print) -> float:
    tag = "[PAPER]" if paper else "[LIVE]"
    if paper:
        log_fn(f"{tag} {side} qty={qty} seg={seg} sid={sid}")
        return 0.0
    hdrs = {"Content-Type":"application/json","access-token":token,"client-id":client_id}
    pl = {"dhanClientId": client_id, "transactionType": side,
          "exchangeSegment": seg, "productType": "INTRADAY",
          "orderType": "MARKET", "validity": "DAY",
          "securityId": sid, "quantity": qty}
    r = requests.post(ORDER_URL, headers=hdrs, json=pl, timeout=15)
    r.raise_for_status()
    return float(r.json().get("avgPrice", 0) or 0)

# ══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET FEED
# ══════════════════════════════════════════════════════════════════════════════
class Feed:
    def __init__(self, client_id: str, token: str,
                 instruments: List[Tuple[str,str]],   # [(sid, seg), ...]
                 on_tick, on_status):
        self.cid   = client_id
        self.tok   = token
        self.insts = instruments
        self._on_tick   = on_tick
        self._on_status = on_status
        self._ws:   Optional[websocket.WebSocketApp] = None
        self._stop  = threading.Event()
        self.status = "Disconnected"
        self.ticks  = 0

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._stop.set()
        if self._ws:
            try: self._ws.close()
            except: pass

    def subscribe(self, sid: str, seg: str):
        """Add a new instrument (e.g. NIFTY option after entry)."""
        if self._ws:
            try:
                self._ws.send(json.dumps({"RequestCode": REQ_SUB,
                    "InstrumentCount": 1,
                    "InstrumentList": [{"ExchangeSegment": seg, "SecurityId": sid}]}))
            except: pass

    def _run(self):
        url = WS_URL_TMPL.format(tok=self.tok, cid=self.cid)
        while not self._stop.is_set():
            try:
                self._ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_msg,
                    on_error=lambda ws, e: self._on_status(f"WS error: {e}"),
                    on_close=lambda ws, *a: self._on_status("WS disconnected"),
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                self._on_status(f"WS exception: {e}")
            if not self._stop.is_set():
                self._on_status("WS reconnecting in 3s...")
                time.sleep(3)

    def _on_open(self, ws):
        BATCH = 100
        total = len(self.insts)
        for i in range(0, total, BATCH):
            batch = self.insts[i:i+BATCH]
            ws.send(json.dumps({"RequestCode": REQ_SUB,
                "InstrumentCount": len(batch),
                "InstrumentList": [{"ExchangeSegment": seg, "SecurityId": sid}
                                   for sid, seg in batch]}))
            if i + BATCH < total: time.sleep(0.1)
        self._on_status(f"WS connected — {total} instruments")

    def _on_msg(self, ws, raw):
        try:
            if isinstance(raw, str): return
            b = bytes(raw)
            if len(b) < 18: return
            hdr = struct.unpack_from(HDR_FMT, b, 0)
            if int(hdr[1]) != RESP_TICK: return
            sid   = str(int(hdr[2]))
            seg_c = int(hdr[3])
            ltp   = float(hdr[5])
            if ltp <= 0: return
            self.ticks += 1
            self._on_tick(sid, seg_c, ltp)
        except: pass

# ══════════════════════════════════════════════════════════════════════════════
#  NIFTY OPTIONS ENGINE
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class NiftyState:
    lots:      int   = 1
    skip:      bool  = False
    position:  str   = "FLAT"   # FLAT / CE / PE
    entry:     float = 0.0
    opt_ltp:   float = 0.0
    opt_sid:   str   = ""
    opt_sym:   str   = ""
    strike:    int   = 0
    status:    str   = "Waiting..."
    sq_done:   bool  = False

    @property
    def pnl(self) -> float:
        if self.position == "FLAT" or self.entry == 0: return 0.0
        return round((self.opt_ltp - self.entry) * self.lots * 65, 2)

class NiftyEngine:
    def __init__(self, state: NiftyState, client_id: str, token: str, feed: "Feed"):
        self.st  = state
        self.cid = client_id
        self.tok = token
        self.fd  = feed
        self._exp_cache:   Optional[List[str]] = None
        self._exp_ts:      float = 0.0
        self._startup_done = False
        self._lock = threading.Lock()

    def process(self, candles: List[dict], sma: int, startup: bool, log_fn=print):
        st = self.st
        if st.skip or st.sq_done: return
        if len(candles) < 2: return

        ha   = compute_ha(candles, sma)
        col  = ha_color(ha[-1])
        spot = float(candles[-1]["close"])

        if startup and not self._startup_done:
            self._startup_done = True
            opt_type = "CE" if col == "GREEN" else "PE"
            log_fn(f"[NIFTY] Startup → {opt_type} ATM {self._atm(spot)} (HA={col})")
            self._enter(opt_type, spot, startup=True, log_fn=log_fn)
            return

        if startup: return

        if col == "DOJI":
            st.status = "DOJI — holding"; return
        want = "CE" if col == "GREEN" else "PE"
        if st.position != "FLAT" and st.position != want:
            log_fn(f"[NIFTY] HA→{col} — reversing to {want}")
            self._exit(log_fn)
            time.sleep(0.3)
            self._enter(want, spot, startup=False, log_fn=log_fn)
        elif st.position == "FLAT":
            self._enter(want, spot, startup=False, log_fn=log_fn)
        else:
            st.status = f"Hold {st.position}"

    def square_off(self, log_fn=print):
        if self.st.position != "FLAT":
            self._exit(log_fn)
        self.st.sq_done = True

    def _atm(self, spot: float) -> int:
        return int(round(spot / NIFTY_STRIKE_STEP)) * NIFTY_STRIKE_STEP

    def _expiries(self) -> List[str]:
        if self._exp_cache and (time.time() - self._exp_ts) < 300:
            return self._exp_cache
        h = {"Content-Type":"application/json","access-token":self.tok,"client-id":self.cid}
        r = requests.post(OC_EXPIRY_URL, headers=h,
                          json={"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I"}, timeout=10)
        d = r.json()
        exps = d.get("data", d.get("expiryList", []))
        self._exp_cache = sorted(exps)[:3]
        self._exp_ts = time.time()
        return self._exp_cache

    def _get_sid(self, opt_type: str, strike: int, expiry: str) -> Tuple[str, int, int]:
        h = {"Content-Type":"application/json","access-token":self.tok,"client-id":self.cid}
        r = requests.post(OC_URL, headers=h,
                          json={"UnderlyingScrip": 13, "UnderlyingSeg": "IDX_I",
                                "ExpiryDate": expiry}, timeout=10)
        d = r.json()
        oc = d.get("data",{}).get("oc",{})
        key = f"{float(strike):.6f}"
        row = oc.get(key, {})
        side_data = row.get("ce" if opt_type=="CE" else "pe", {})
        sid = str(side_data.get("security_id",""))
        lp  = float(side_data.get("last_price", 0))
        lot = int(d.get("data",{}).get("lot_size", 65))
        return sid, lot, int(lp)

    def _enter(self, opt_type: str, spot: float, startup: bool, log_fn=print):
        st = self.st
        try:
            exps   = self._expiries()
            strike = self._atm(spot)
            sid, lot, lp = self._get_sid(opt_type, strike, exps[0])
            sym = f"NIFTY{exps[0].replace('-','')}{strike}{opt_type}"
            log_fn(f"[NIFTY] Enter {opt_type} {sym} qty={lot*st.lots} ltp~{lp}")
            with self._lock:
                st.position = opt_type
                st.entry    = lp
                st.opt_sid  = sid
                st.opt_sym  = sym
                st.strike   = strike
                st.status   = f"{'↑' if opt_type=='CE' else '↓'}{opt_type} {strike}"
            self.fd.subscribe(sid, NIFTY_FNO_SEG)
        except Exception as e:
            log_fn(f"[NIFTY] Entry error: {e}")

    def _exit(self, log_fn=print):
        st = self.st
        pnl = st.pnl
        log_fn(f"[NIFTY] Exit {st.opt_sym} P&L=₹{pnl:+.2f}")
        with self._lock:
            st.position = "FLAT"
            st.entry    = 0.0
            st.opt_sid  = ""
            st.opt_sym  = ""
            st.status   = "FLAT"

# ══════════════════════════════════════════════════════════════════════════════
#  STRATEGY ENGINE
# ══════════════════════════════════════════════════════════════════════════════
class Engine:
    def __init__(self, client_id: str, token: str,
                 instruments: List[InstrState],
                 iv_sec: int, sma: int, paper: bool,
                 nse_sq: str, mcx_sq: str,
                 nifty_state: Optional[NiftyState] = None,
                 log_fn=print):
        self.cid    = client_id
        self.tok    = token
        self.insts  = instruments
        self.iv_sec = iv_sec
        self.sma    = sma
        self.paper  = paper
        self.nse_sq = nse_sq
        self.mcx_sq = mcx_sq
        self.log    = log_fn

        # Index: "seg:sid" → InstrState
        self._map:  Dict[str, InstrState] = {}
        for st in instruments:
            self._map[f"{st.cfg.seg}:{st.cfg.sid}"] = st
            self._map[st.cfg.sid] = st

        # WS feed
        feed_insts = [(st.cfg.sid, st.cfg.seg) for st in instruments]
        feed_insts.append((NIFTY_SID, NIFTY_SEG))
        self._feed = Feed(client_id, token, feed_insts,
                          on_tick=self._on_tick, on_status=self._on_status)

        # Aggregators: "seg:sid" → CandleAgg
        self._aggs: Dict[str, CandleAgg] = {
            f"{st.cfg.seg}:{st.cfg.sid}": CandleAgg(iv_sec) for st in instruments}
        self._aggs[f"{NIFTY_SEG}:{NIFTY_SID}"] = CandleAgg(iv_sec)

        # NIFTY engine
        self._nifty_eng: Optional[NiftyEngine] = None
        if nifty_state and not nifty_state.skip:
            self._nifty_eng = NiftyEngine(nifty_state, client_id, token, self._feed)
        self._nifty_st = nifty_state

        # Runtime
        self._lock    = threading.Lock()
        self._stop    = threading.Event()
        self._started = False
        self.ws_status  = "Not started"
        self.ws_ticks   = 0
        self.next_candle = "-"
        self._startup_done: set = set()

    def start(self):
        threading.Thread(target=self._run, daemon=True).start()

    def stop(self):
        self._stop.set()
        self._feed.stop()

    def sq_off_all(self):
        ORDER_GAP = 0.12
        for st in self.insts:
            if st.position != "FLAT" and not st.sq_off_done:
                self._close(st, "SqOff")
                st.sq_off_done = True
                time.sleep(ORDER_GAP)
        if self._nifty_eng and self._nifty_st:
            self._nifty_eng.square_off(self.log)

    # ── WebSocket callbacks ──────────────────────────────────────────────────
    def _on_tick(self, sid: str, seg_code: int, ltp: float):
        # Segment code map
        SEG_MAP = {1:"NSE_EQ", 2:"NSE_FNO", 3:"BSE_EQ", 8:"MCX_COMM", 9:"IDX_I"}
        seg = SEG_MAP.get(seg_code, "")
        key = f"{seg}:{sid}" if seg else sid
        now = time.time()

        st = self._map.get(key) or self._map.get(sid)
        if st:
            with self._lock:
                st.ltp       = round(ltp, 2)
                st.last_tick = now
            agg = self._aggs.get(key) or self._aggs.get(f"{st.cfg.seg}:{st.cfg.sid}")
            if agg: agg.on_tick(ltp, now)
            return

        # NIFTY spot
        if sid == NIFTY_SID:
            agg = self._aggs.get(f"{NIFTY_SEG}:{NIFTY_SID}")
            if agg: agg.on_tick(ltp, now)
            return

        # NIFTY option
        if self._nifty_st and sid == self._nifty_st.opt_sid:
            with self._lock:
                self._nifty_st.opt_ltp = round(ltp, 2)

    def _on_status(self, msg: str):
        self.ws_status = msg
        self.log(f"[WS] {msg}")

    # ── Main loop ────────────────────────────────────────────────────────────
    def _run(self):
        # Seed aggregators
        self.log(f"[Engine] Seeding {len(self.insts)+1} instruments...")
        rest_iv = "1" if self.iv_sec <= 60 else "5" if self.iv_sec <= 300 else "15"
        all_seed = list(self.insts) + [None]   # None = NIFTY spot
        for i, st in enumerate(all_seed):
            if self._stop.is_set(): break
            try:
                if st is None:
                    candles = fetch_ohlc(self.cid, self.tok, NIFTY_SID, NIFTY_SEG, rest_iv)
                    key = f"{NIFTY_SEG}:{NIFTY_SID}"
                    nm  = "NIFTY"
                else:
                    candles = fetch_ohlc(self.cid, self.tok, st.cfg.sid, st.cfg.seg, rest_iv)
                    key = f"{st.cfg.seg}:{st.cfg.sid}"
                    nm  = st.cfg.name
                self._aggs[key] = CandleAgg(self.iv_sec, seed=candles[-100:])
                self.log(f"[seed] {nm} {len(candles)} bars")
            except Exception as e:
                nm2 = "NIFTY" if st is None else (st.cfg.name if st else "?")
                self.log(f"[seed ERR] {nm2}: {e}")
            if (i + 1) % 5 == 0: time.sleep(1.0)

        self.log("[Engine] Seed done — starting WebSocket")
        self._feed.start()
        time.sleep(2)   # let WS connect

        # Startup signals
        self._process(startup=True)
        self._set_next()

        while not self._stop.is_set():
            self._wait_boundary()
            if self._stop.is_set(): break
            self._check_sq()
            self._process(startup=False)
            self._set_next()

        self.log("[Engine] Stopped")

    def _interval_sec(self) -> int: return self.iv_sec

    def _set_next(self):
        nxt = (int(time.time()) // self.iv_sec + 1) * self.iv_sec
        self.next_candle = datetime.fromtimestamp(nxt).strftime("%H:%M:%S")

    def _wait_boundary(self):
        nxt = (int(time.time()) // self.iv_sec + 1) * self.iv_sec
        while time.time() < nxt and not self._stop.is_set():
            time.sleep(0.05)

    def _check_sq(self):
        t = datetime.now().strftime("%H:%M")
        ORDER_GAP = 0.12
        for st in self.insts:
            if st.sq_off_done: continue
            sq_t = self.mcx_sq if st.cfg.is_mcx else self.nse_sq
            if t >= sq_t and st.position != "FLAT":
                self.log(f"[SqOff] {st.cfg.name}")
                self._close(st, "AutoSqOff"); st.sq_off_done = True
                time.sleep(ORDER_GAP)
        if self._nifty_st and not self._nifty_st.sq_done:
            if t >= self.nse_sq and self._nifty_st.position != "FLAT":
                self.log("[NIFTY] Auto sq-off")
                self._nifty_eng.square_off(self.log) if self._nifty_eng else None

    def _process(self, startup: bool):
        ORDER_GAP = 0.12
        # NIFTY first
        if self._nifty_eng and self._nifty_st and not self._nifty_st.sq_done:
            candles = self._aggs[f"{NIFTY_SEG}:{NIFTY_SID}"].candles()
            if len(candles) >= 2:
                self._nifty_eng.process(candles, self.sma, startup, self.log)
            time.sleep(ORDER_GAP)

        # MCX then NSE
        mcx = [s for s in self.insts if s.cfg.is_mcx and not s.skip and not s.sq_off_done]
        nse = [s for s in self.insts if not s.cfg.is_mcx and not s.skip and not s.sq_off_done]
        for st in mcx + nse:
            if self._stop.is_set(): break
            key = f"{st.cfg.seg}:{st.cfg.sid}"
            agg = self._aggs.get(key)
            if not agg or not agg.ready(): continue
            candles = agg.candles()
            if len(candles) < 2: continue

            ha      = compute_ha(candles, self.sma)
            last_ha = ha[-1]
            col     = ha_color(last_ha)
            bar_ts  = datetime.fromtimestamp(last_ha["bucket"]).strftime("%H:%M")

            with self._lock:
                st.ha_open  = round(last_ha["open"],  2)
                st.ha_close = round(last_ha["close"], 2)
                st.color    = col
                st.bar_time = bar_ts
                st.signal   = "" if col == "DOJI" else ("BUY" if col=="GREEN" else "SELL")

            if col == "DOJI":
                with self._lock: st.status = "DOJI"
                continue

            signal = "BUY" if col == "GREEN" else "SELL"
            need = "LONG" if signal == "BUY" else "SHORT"

            # Startup: enter immediately
            if startup and st.cfg.name not in self._startup_done:
                self._startup_done.add(st.cfg.name)
                if st.position == "FLAT":
                    self._open(st, signal)
                elif st.position != need:
                    self._reverse(st, signal)
                time.sleep(ORDER_GAP)
                continue
            if startup: continue

            # Running: react to direction change
            if st.position != need:
                if st.position == "FLAT": self._open(st, signal)
                else: self._reverse(st, signal)
                time.sleep(ORDER_GAP)
            else:
                with self._lock:
                    st.status = f"↑LONG" if need=="LONG" else "↓SHORT"

    def _open(self, st: InstrState, signal: str):
        side = "BUY" if signal=="BUY" else "SELL"
        price = place_order(self.cid, self.tok, st.cfg.seg, st.cfg.sid,
                            side, st.qty, self.paper, self.log)
        tag = "PAPER" if self.paper else "LIVE"
        with self._lock:
            st.position = "LONG" if signal=="BUY" else "SHORT"
            st.entry    = price or st.ltp
            st.status   = f"↑LONG" if st.position=="LONG" else "↓SHORT"
        self.log(f"[{tag}] {st.cfg.name} {side} qty={st.qty} @{st.entry:.2f}")

    def _reverse(self, st: InstrState, signal: str):
        pnl = st.unrealised_pnl
        close_side = "SELL" if st.position=="LONG" else "BUY"
        self.log(f"[CLOSE] {st.cfg.name} P&L=₹{pnl:+.2f}")
        place_order(self.cid, self.tok, st.cfg.seg, st.cfg.sid,
                    close_side, st.qty, self.paper, self.log)
        self._open(st, signal)

    def _close(self, st: InstrState, reason: str):
        pnl = st.unrealised_pnl
        side = "SELL" if st.position=="LONG" else "BUY"
        self.log(f"[CLOSE/{reason}] {st.cfg.name} P&L=₹{pnl:+.2f}")
        place_order(self.cid, self.tok, st.cfg.seg, st.cfg.sid,
                    side, st.qty, self.paper, self.log)
        with self._lock:
            st.position = "FLAT"; st.entry = 0.0; st.status = "FLAT"

# ══════════════════════════════════════════════════════════════════════════════
#  GUI
# ══════════════════════════════════════════════════════════════════════════════
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

COLS = [
    ("",     24),   # checkbox
    ("Instrument", 110),
    ("HA-O",  72),
    ("HA-C",  72),
    ("HA",    80),
    ("Sig",   52),
    ("Pos",   60),
    ("Entry", 76),
    ("Qty",   48),
    ("LTP",   76),
    ("P&L",   80),
    ("Bar",   50),
    ("Status",160),
]
CI_CHK, CI_NAME, CI_HAO, CI_HAC, CI_COL, CI_SIG, CI_POS, CI_ENT, CI_QTY, CI_LTP, CI_PNL, CI_BAR, CI_STA = range(13)
HDR_H   = 28
ROW_H   = 26
TOTAL_W = sum(w for _, w in COLS) + 24

class Row:
    def __init__(self, parent, st: InstrState, bg: str,
                 on_click, qty_var: Optional[ctk.IntVar] = None):
        self.st      = st
        self._qty_var = qty_var
        self._labels: Dict[int, ctk.CTkLabel] = {}
        self._qty_ent: Optional[ctk.CTkEntry] = None

        frame = ctk.CTkFrame(parent, fg_color=bg, height=ROW_H, corner_radius=0)
        frame.pack(fill="x")
        frame.pack_propagate(False)
        frame.bind("<Button-1>", lambda e: on_click(self))
        self._frame = frame

        x = 4
        # Checkbox
        self._chk = ctk.BooleanVar(value=not st.skip)
        cb = ctk.CTkCheckBox(frame, text="", variable=self._chk,
            width=COLS[CI_CHK][1], checkbox_width=14, checkbox_height=14,
            command=lambda: setattr(st, "skip", not self._chk.get()))
        cb.place(x=x, rely=0.5, anchor="w"); x += COLS[CI_CHK][1] + 2

        for ci in range(CI_NAME, len(COLS)):
            w = COLS[ci][1]
            if ci == CI_QTY and qty_var is not None:
                ent = ctk.CTkEntry(frame, textvariable=qty_var,
                    width=w, justify="center", height=20,
                    font=(FONT[0], 10),
                    fg_color="#EFF6FF", border_color=BD, border_width=1)
                ent.place(x=x, rely=0.5, anchor="w")
                self._qty_ent = ent
                lbl = ctk.CTkLabel(frame, text="-", width=w, anchor="center",
                    font=FONT, text_color=GY)
                lbl.place(x=x, rely=0.5, anchor="w")
                lbl.place_forget()
                self._labels[ci] = lbl
            else:
                lbl = ctk.CTkLabel(frame, text="-", width=w, anchor="center",
                    font=FONT, text_color=TX)
                lbl.place(x=x, rely=0.5, anchor="w")
                lbl.bind("<Button-1>", lambda e: on_click(self))
                self._labels[ci] = lbl
            x += w + 2

    def set_selected(self, sel: bool):
        self._frame.configure(fg_color=("#DBEAFE" if sel else
                               (ROW_A if self._frame.cget("fg_color") in (ROW_A,"#DBEAFE") else ROW_B)))

    def update(self):
        st = self.st
        def lbl(ci, txt, clr=TX):
            w = self._labels.get(ci)
            if w: w.configure(text=str(txt), text_color=clr)

        if st.skip:
            for ci in range(CI_NAME, len(COLS)):
                lbl(ci, "--", GY)
            lbl(CI_NAME, st.cfg.name, GY); return

        lbl(CI_NAME, st.cfg.name)
        lbl(CI_HAO,  f"{st.ha_open:.2f}"  if st.ha_open  else "-")
        lbl(CI_HAC,  f"{st.ha_close:.2f}" if st.ha_close else "-")

        col_sym = {"GREEN":"▲ GREEN","RED":"▼ RED","DOJI":"— DOJI"}.get(st.color, "-")
        lbl(CI_COL, col_sym, {"GREEN":GR,"RED":RD,"DOJI":"#D97706"}.get(st.color, GY))
        lbl(CI_SIG, st.signal, {"BUY":GR,"SELL":RD}.get(st.signal, GY))
        lbl(CI_POS, st.position, {"LONG":GR,"SHORT":RD,"FLAT":GY}.get(st.position, GY))
        lbl(CI_ENT, f"{st.entry:.2f}"  if st.entry  else "-")
        lbl(CI_LTP, f"{st.ltp:.2f}"    if st.ltp    else "-")

        pnl = st.unrealised_pnl
        lbl(CI_PNL, f"₹{pnl:+.2f}" if st.position!="FLAT" else "-",
            GR if pnl > 0 else (RD if pnl < 0 else GY))
        lbl(CI_BAR, st.bar_time)

        # Tick freshness in status
        age = time.time() - st.last_tick
        tick = f"●{datetime.fromtimestamp(st.last_tick).strftime('%H:%M:%S')}" if age<5 \
               else f"○{int(age)}s" if age<30 else "✕NO TICK"
        sta = st.status if len(st.status)<14 else st.status[:13]
        lbl(CI_STA, f"{sta} {tick}", GR if "LONG" in st.status else RD if "SHORT" in st.status else GY)


class NiftyRow:
    def __init__(self, parent, st: NiftyState, bg: str, on_click):
        self.st = st
        self._labels: Dict[str, ctk.CTkLabel] = {}

        frame = ctk.CTkFrame(parent, fg_color=bg, height=ROW_H, corner_radius=0)
        frame.pack(fill="x")
        frame.pack_propagate(False)
        frame.bind("<Button-1>", lambda e: on_click(self))
        self._frame = frame

        x = 4
        cb = ctk.CTkCheckBox(frame, text="", variable=ctk.BooleanVar(value=not st.skip),
            width=COLS[CI_CHK][1], checkbox_width=14, checkbox_height=14,
            command=lambda: None)
        cb.place(x=x, rely=0.5, anchor="w"); x += COLS[CI_CHK][1] + 2

        for ci in range(CI_NAME, len(COLS)):
            w = COLS[ci][1]
            lbl = ctk.CTkLabel(frame, text="-", width=w, anchor="center",
                               font=FONT, text_color=TD)
            lbl.place(x=x, rely=0.5, anchor="w")
            lbl.bind("<Button-1>", lambda e: on_click(self))
            self._labels[ci] = lbl
            x += w + 2

        self._labels[CI_NAME].configure(text="NIFTY OPT", text_color=AC,
                                        font=(FONT[0], FONT[1], "bold"))

    def set_selected(self, sel: bool):
        self._frame.configure(fg_color="#DBEAFE" if sel else ROW_A)

    def update(self):
        st = self.st
        def lbl(ci, txt, clr=TX):
            w = self._labels.get(ci)
            if w: w.configure(text=str(txt), text_color=clr)

        lbl(CI_SIG, st.position, {"CE":GR,"PE":RD,"FLAT":GY}.get(st.position, GY))
        lbl(CI_POS, st.position, {"CE":GR,"PE":RD,"FLAT":GY}.get(st.position, GY))
        lbl(CI_ENT, f"{st.entry:.2f}" if st.entry else "-")
        lbl(CI_LTP, f"{st.opt_ltp:.2f}" if st.opt_ltp else "-")
        pnl = st.pnl
        lbl(CI_PNL, f"₹{pnl:+.2f}" if st.position!="FLAT" else "-",
            GR if pnl>0 else RD if pnl<0 else GY)
        sym = st.opt_sym or "NIFTY ATM"
        lbl(CI_STA, st.status)
        lbl(CI_HAO, f"{st.strike}" if st.strike else "-")


class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund — HA + SMA Strategy Trader  v2.0")
        self.geometry("1420x860")
        self.configure(fg_color=BG)
        self.resizable(True, True)

        # State
        self.engine:   Optional[Engine] = None
        self.instruments: List[InstrState] = []
        self.nifty_st: Optional[NiftyState] = None
        self._master_rows: List[dict] = []
        self._rows:    List[Row] = []
        self._nifty_row: Optional[NiftyRow] = None
        self._sel_row  = None
        self._running  = False
        self._stock_qty_vars: Dict[str, ctk.IntVar] = {}

        # Settings vars — defined BEFORE _build()
        self.tf_var          = ctk.StringVar(value="1")
        self.sma_var         = ctk.IntVar(value=1)
        self.paper_var       = ctk.BooleanVar(value=True)
        self.nse_sq_var      = ctk.StringVar(value="15:15")
        self.mcx_sq_var      = ctk.StringVar(value="23:25")
        self.gold_lots_var   = ctk.IntVar(value=1)
        self.silv_lots_var   = ctk.IntVar(value=1)
        self.crude_lots_var  = ctk.IntVar(value=1)
        self.zinc_lots_var   = ctk.IntVar(value=1)
        self.gp_lots_var     = ctk.IntVar(value=1)
        self.nifty_en_var    = ctk.BooleanVar(value=False)
        self.nifty_lots_var  = ctk.IntVar(value=1)
        self.default_qty_var = ctk.IntVar(value=1)

        self._build()
        self.after(500, self._preload)
        self.after(800, self._tick)

    # ── Build ────────────────────────────────────────────────────────────────
    def _build(self):
        self._build_header()
        tabs = ctk.CTkTabview(self, fg_color=BG,
            segmented_button_selected_color=AC,
            segmented_button_selected_hover_color=ACL,
            segmented_button_unselected_color=CARD)
        tabs.pack(fill="both", expand=True, padx=8, pady=(0,6))
        for t in ["Settings", "Live Strategy", "Log"]:
            tabs.add(t)
        self._tabs = tabs
        self._build_settings(tabs.tab("Settings"))
        self._build_live(tabs.tab("Live Strategy"))
        self._build_log(tabs.tab("Log"))

    def _build_header(self):
        hdr = ctk.CTkFrame(self, fg_color=HBG, height=52, corner_radius=0)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        ctk.CTkLabel(hdr, text="BALFUND", font=("Segoe UI",20,"bold"),
            text_color=GOLD).pack(side="left", padx=16)
        ctk.CTkLabel(hdr, text="Heikin-Ashi + SMA  Strategy Trader",
            font=("Segoe UI",12), text_color="#93C5FD").pack(side="left", padx=4)
        self._lbl_pnl  = ctk.CTkLabel(hdr, text="Net P&L: ₹0.00",
            font=("Segoe UI",13,"bold"), text_color=GOLD)
        self._lbl_pnl.pack(side="right", padx=16)
        self._lbl_ws   = ctk.CTkLabel(hdr, text="WS: --",
            font=("Segoe UI",10), text_color="#93C5FD")
        self._lbl_ws.pack(side="right", padx=10)
        self._lbl_mode = ctk.CTkLabel(hdr, text="PAPER",
            font=("Segoe UI",11,"bold"), text_color="#FDE68A")
        self._lbl_mode.pack(side="right", padx=10)
        self._lbl_clk  = ctk.CTkLabel(hdr, text="",
            font=("Segoe UI",10), text_color="#93C5FD")
        self._lbl_clk.pack(side="right", padx=10)

    def _build_settings(self, parent):
        parent.configure(fg_color=BG)
        parent.grid_columnconfigure((0,1,2,3), weight=1)

        def card(col, title):
            f = ctk.CTkFrame(parent, fg_color=CARD, corner_radius=8,
                             border_width=1, border_color=BD)
            f.grid(row=0, column=col, padx=8, pady=10, sticky="nsew")
            ctk.CTkLabel(f, text=title, font=("Segoe UI",12,"bold"),
                         text_color=AC).pack(pady=(10,6), anchor="w", padx=12)
            return f

        # ── Col 0: Timeframe + SMA ────────────────────────────────────────
        tf = card(0, "Candle Timeframe")
        for v, l in [("5s","5 Seconds (WS)"),("1","1 Minute"),
                     ("5","5 Minutes"),("15","15 Minutes")]:
            ctk.CTkRadioButton(tf, text=l, variable=self.tf_var, value=v,
                font=FONT).pack(anchor="w", padx=20, pady=4)
        ctk.CTkFrame(tf, fg_color=BD, height=1).pack(fill="x", padx=12, pady=6)
        ctk.CTkLabel(tf, text="HA Smoothing (SMA period)",
            font=FONT, text_color=TD).pack(anchor="w", padx=20)
        _sr = ctk.CTkFrame(tf, fg_color="transparent")
        _sr.pack(anchor="w", padx=20, pady=(3,10))
        ctk.CTkEntry(_sr, textvariable=self.sma_var, width=60, justify="center",
            font=FONT).pack(side="left")
        ctk.CTkLabel(_sr, text=" (1=standard, 21=smoothed)",
            font=("Segoe UI",10), text_color=GY).pack(side="left", padx=4)

        # ── Col 1: Quantities ─────────────────────────────────────────────
        qf = card(1, "Quantity per Trade")
        _qr = ctk.CTkFrame(qf, fg_color="transparent")
        _qr.pack(fill="x", padx=12, pady=3)
        ctk.CTkLabel(_qr, text="Default NSE qty:", width=130, anchor="w",
            font=FONT, text_color=TD).pack(side="left")
        ctk.CTkEntry(_qr, textvariable=self.default_qty_var, width=60,
            justify="center", font=FONT).pack(side="left", padx=4)
        ctk.CTkLabel(_qr, text="shares", font=("Segoe UI",10),
            text_color=GY).pack(side="left")

        ctk.CTkFrame(qf, fg_color=BD, height=1).pack(fill="x", padx=12, pady=4)
        for lbl, var, unit in [
            ("GOLDTEN:",    self.gold_lots_var,  "lots"),
            ("SILVERMICRO:",self.silv_lots_var,  "lots"),
            ("CRUDEOILM:",  self.crude_lots_var, "lots"),
            ("ZINCMINI:",   self.zinc_lots_var,  "lots"),
            ("GOLDPETAL:",  self.gp_lots_var,    "lots"),
        ]:
            r = ctk.CTkFrame(qf, fg_color="transparent")
            r.pack(fill="x", padx=12, pady=2)
            ctk.CTkLabel(r, text=lbl, width=130, anchor="w",
                font=FONT, text_color=TD).pack(side="left")
            ctk.CTkEntry(r, textvariable=var, width=60, justify="center",
                font=FONT).pack(side="left", padx=4)
            ctk.CTkLabel(r, text=unit, font=("Segoe UI",10),
                text_color=GY).pack(side="left")

        # ── Col 2: NIFTY + Sessions ───────────────────────────────────────
        nf = card(2, "NIFTY Options")
        ctk.CTkSwitch(nf, text="Trade NIFTY ATM Options",
            variable=self.nifty_en_var, font=FONT,
            button_color=AC, progress_color=ACL).pack(anchor="w", padx=12, pady=4)
        _nr = ctk.CTkFrame(nf, fg_color="transparent")
        _nr.pack(fill="x", padx=12, pady=3)
        ctk.CTkLabel(_nr, text="Lots:", width=60, anchor="w",
            font=FONT, text_color=TD).pack(side="left")
        ctk.CTkEntry(_nr, textvariable=self.nifty_lots_var, width=60,
            justify="center", font=FONT).pack(side="left", padx=4)

        ctk.CTkFrame(nf, fg_color=BD, height=1).pack(fill="x", padx=12, pady=8)
        ctk.CTkLabel(nf, text="Session Square-off",
            font=("Segoe UI",11,"bold"), text_color=TD).pack(anchor="w", padx=12)
        for lbl2, var2 in [("NSE/NIFTY:", self.nse_sq_var),
                            ("MCX:",       self.mcx_sq_var)]:
            r2 = ctk.CTkFrame(nf, fg_color="transparent")
            r2.pack(fill="x", padx=12, pady=3)
            ctk.CTkLabel(r2, text=lbl2, width=80, anchor="w",
                font=FONT, text_color=TD).pack(side="left")
            ctk.CTkEntry(r2, textvariable=var2, width=70, justify="center",
                font=FONT).pack(side="left", padx=4)

        # ── Col 3: Mode + Start/Stop ──────────────────────────────────────
        cf = card(3, "Control")
        ctk.CTkSwitch(cf, text="Paper Mode (safe)",
            variable=self.paper_var, font=FONT,
            button_color=AC, progress_color=ACL).pack(anchor="w", padx=12, pady=6)
        ctk.CTkFrame(cf, fg_color=BD, height=1).pack(fill="x", padx=12, pady=4)
        self._lbl_status = ctk.CTkLabel(cf, text="Ready",
            font=("Segoe UI",11), text_color=TD, wraplength=180)
        self._lbl_status.pack(padx=12, pady=4)
        self._btn_start = ctk.CTkButton(cf, text="▶  START",
            font=("Segoe UI",13,"bold"), fg_color=GR, hover_color="#047857",
            command=self._start, height=36)
        self._btn_start.pack(fill="x", padx=12, pady=4)
        self._btn_stop = ctk.CTkButton(cf, text="■  STOP",
            font=("Segoe UI",13,"bold"), fg_color=RD, hover_color="#991B1B",
            command=self._stop, state="disabled", height=36)
        self._btn_stop.pack(fill="x", padx=12, pady=4)
        ctk.CTkButton(cf, text="⬛  Square Off All",
            font=FONT, fg_color="#374151", hover_color="#111827",
            command=self._sq_all, height=30).pack(fill="x", padx=12, pady=4)

    def _build_live(self, parent):
        parent.configure(fg_color=BG)
        # Toolbar
        tb = ctk.CTkFrame(parent, fg_color=CARD, corner_radius=6,
                          border_width=1, border_color=BD, height=36)
        tb.pack(fill="x", padx=6, pady=(6,2)); tb.pack_propagate(False)
        self._lbl_sel = ctk.CTkLabel(tb, text="Click a row to select",
            font=FONT, text_color=TD)
        self._lbl_sel.pack(side="left", padx=12)
        self._lbl_next = ctk.CTkLabel(tb, text="Next candle: --",
            font=FONT, text_color=TD)
        self._lbl_next.pack(side="right", padx=12)
        self._lbl_ws2 = ctk.CTkLabel(tb, text="WS: --",
            font=FONT, text_color=GY)
        self._lbl_ws2.pack(side="right", padx=12)

        # Column header
        hf = ctk.CTkFrame(parent, fg_color=HBG, height=HDR_H, corner_radius=0)
        hf.pack(fill="x", padx=6); hf.pack_propagate(False)
        x = 4
        for ci, (col_title, w) in enumerate(COLS):
            if col_title:
                ctk.CTkLabel(hf, text=col_title, width=w, anchor="center",
                    font=("Segoe UI",10,"bold"), text_color="#93C5FD").place(
                    x=x, rely=0.5, anchor="w")
            x += w + 2

        # Summary bar
        self._lbl_summary = ctk.CTkLabel(parent, text="",
            font=("Segoe UI",11,"bold"), text_color=TX, fg_color=CARD,
            corner_radius=4)
        self._lbl_summary.pack(fill="x", padx=6, pady=(1,2))

        # Scrollable rows
        self._scroll = ctk.CTkScrollableFrame(parent, fg_color=BG,
            scrollbar_button_color=BD)
        self._scroll.pack(fill="both", expand=True, padx=6, pady=2)

    def _build_log(self, parent):
        parent.configure(fg_color=BG)
        tb = ctk.CTkFrame(parent, fg_color=CARD, corner_radius=6, height=32)
        tb.pack(fill="x", padx=6, pady=(6,2)); tb.pack_propagate(False)
        ctk.CTkButton(tb, text="Clear", width=70, height=22, font=FONT,
            fg_color=BD, text_color=TX, hover_color="#E5E7EB",
            command=lambda: self._log_box.delete("1.0","end")).pack(
            side="right", padx=6, pady=4)
        self._log_box = ctk.CTkTextbox(parent, font=("Consolas",11,"bold"),
            fg_color=CARD, text_color=TX, border_width=1, border_color=BD)
        self._log_box.pack(fill="both", expand=True, padx=6, pady=(2,6))

    # ── Instrument rows ──────────────────────────────────────────────────────
    def _build_rows(self):
        for w in self._scroll.winfo_children(): w.destroy()
        self._rows.clear(); self._nifty_row = None; self._sel_row = None

        n = [0]   # row counter for alternating colours

        def add_row(st, qty_var=None):
            bg = ROW_A if n[0] % 2 == 0 else ROW_B
            row = Row(self._scroll, st, bg, self._on_row_click, qty_var)
            self._rows.append(row); n[0] += 1

        # NIFTY row first
        if self.nifty_st:
            self._nifty_row = NiftyRow(self._scroll, self.nifty_st,
                ROW_A, self._on_row_click)
            n[0] += 1

        # MCX
        for st in self.instruments:
            if st.cfg.is_mcx: add_row(st)

        # NSE
        for st in self.instruments:
            if not st.cfg.is_mcx:
                add_row(st, self._stock_qty_vars.get(st.cfg.name))

    def _on_row_click(self, row):
        if self._sel_row and self._sel_row is not row:
            self._sel_row.set_selected(False)
        self._sel_row = row
        row.set_selected(True)
        if isinstance(row, NiftyRow):
            self._lbl_sel.configure(text=f"Selected: NIFTY OPTIONS ({row.st.position})")
        else:
            st = row.st
            self._lbl_sel.configure(text=f"Selected: {st.cfg.name} ({st.position})")

    # ── Logging ──────────────────────────────────────────────────────────────
    def _log(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        line = f"[{ts}] {msg}\n"
        self.after(0, lambda: (self._log_box.insert("end", line),
                               self._log_box.see("end")))

    def _set_status(self, msg: str, color: str = TD):
        self.after(0, lambda: self._lbl_status.configure(text=msg, text_color=color))

    # ── Preload ──────────────────────────────────────────────────────────────
    def _preload(self):
        threading.Thread(target=self._do_preload, daemon=True).start()

    def _do_preload(self):
        self._log("[AUTO] Pre-loading instruments...")
        try:
            rows = load_master(self._log)
            instruments = self._build_instrument_list(rows)
            self._master_rows = rows
            self.after(0, lambda: self._on_preloaded(instruments))
        except Exception as e:
            self._log(f"[PRE-LOAD ERR] {e}")

    def _build_instrument_list(self, rows: List[dict]) -> List[InstrState]:
        instruments = []
        default_qty = self.default_qty_var.get()

        # MCX
        for sym, lv in [("GOLDTEN",self.gold_lots_var),("SILVERMICRO",self.silv_lots_var),
                        ("CRUDEOILM",self.crude_lots_var),("ZINCMINI",self.zinc_lots_var),
                        ("GOLDPETAL",self.gp_lots_var)]:
            m = resolve_mcx(rows, sym, self._log)
            if not m: continue
            mult = MCX_LOT_MULTIPLIERS.get(sym, 1)
            instruments.append(InstrState(
                cfg=InstrCfg(name=sym, seg="MCX_COMM", sid=m["sid"],
                             is_mcx=True, lot_mult=mult,
                             trading_symbol=m["trading_symbol"],
                             expiry=m["expiry"]),
                qty=lv.get()))

        # NSE — build index once, then O(1) lookups
        self._log("  Building NSE index...")
        nse_idx = _build_nse_index(rows)
        self._log(f"  NSE index built — {len(nse_idx)} symbols")
        found, missing = 0, []
        for sym in NSE_STOCKS:
            sid = nse_idx.get(sym.upper())
            if not sid:
                missing.append(sym)
                continue
            if sym not in self._stock_qty_vars:
                self._stock_qty_vars[sym] = ctk.IntVar(value=default_qty)
            instruments.append(InstrState(
                cfg=InstrCfg(name=sym, seg="NSE_EQ", sid=sid),
                qty=self._stock_qty_vars[sym].get()))
            found += 1
        self._log(f"  ✓ {found} NSE stocks resolved")
        if missing:
            self._log(f"  ✗ Not found ({len(missing)}): {', '.join(missing[:10])}"
                      + (f"... +{len(missing)-10} more" if len(missing)>10 else ""))

        self._log(f"  {len(instruments)} instruments ready.")
        return instruments

    def _on_preloaded(self, instruments: List[InstrState]):
        self.instruments = instruments
        if self.nifty_en_var.get():
            self.nifty_st = NiftyState(lots=self.nifty_lots_var.get(),
                                       skip=False)
        self._build_rows()
        self._tabs.set("Live Strategy")
        self._log(f"[AUTO] {len(instruments)} instruments loaded — ready to START")

    # ── Start / Stop ─────────────────────────────────────────────────────────
    def _start(self):
        if self._running: return
        if not self.instruments:
            self._set_status("No instruments loaded", RD); return

        cid, tok = load_token()
        if not cid or not tok:
            self._set_status("No token found", RD); return

        tf  = self.tf_var.get()
        iv  = 5 if tf == "5s" else int(tf) * 60
        sma = max(1, self.sma_var.get())

        # Sync qtys from editable vars
        for st in self.instruments:
            if not st.cfg.is_mcx and st.cfg.name in self._stock_qty_vars:
                st.qty = self._stock_qty_vars[st.cfg.name].get()

        # NIFTY state
        if self.nifty_en_var.get() and not self.nifty_st:
            self.nifty_st = NiftyState(lots=self.nifty_lots_var.get(), skip=False)
        elif not self.nifty_en_var.get():
            self.nifty_st = None

        self.engine = Engine(
            client_id=cid, token=tok,
            instruments=self.instruments,
            iv_sec=iv, sma=sma,
            paper=self.paper_var.get(),
            nse_sq=self.nse_sq_var.get(),
            mcx_sq=self.mcx_sq_var.get(),
            nifty_state=self.nifty_st,
            log_fn=self._log,
        )
        self.engine.start()
        self._running = True
        self._btn_start.configure(state="disabled", text="RUNNING")
        self._btn_stop.configure(state="normal")
        mode = "PAPER" if self.paper_var.get() else "LIVE"
        self._lbl_mode.configure(text=mode,
            text_color="#FDE68A" if mode=="PAPER" else RD)
        tf_disp = tf if tf=="5s" else f"{tf}m"
        self._set_status(
            f"Running | {tf_disp} WS | {len(self.instruments)} instruments | {mode}",
            GR)
        self._tabs.set("Live Strategy")

    def _stop(self):
        if self.engine: self.engine.stop()
        self._running = False
        self._btn_start.configure(state="normal", text="▶  START")
        self._btn_stop.configure(state="disabled")
        self._set_status("Stopped", TD)

    def _sq_all(self):
        if self.engine:
            threading.Thread(target=self.engine.sq_off_all, daemon=True).start()
            self._log("[SqOff] Square-off all triggered")

    # ── GUI tick ─────────────────────────────────────────────────────────────
    def _tick(self):
        now = datetime.now()
        self._lbl_clk.configure(text=now.strftime("%d %b %Y  %H:%M:%S"))

        if self.engine:
            ws = self.engine.ws_status
            ticks = self.engine.ws_ticks
            ws_color = GR if "connected" in ws.lower() else RD if "error" in ws.lower() else GY
            self._lbl_ws.configure(text=f"WS: {ws[:25]}", text_color=ws_color)
            self._lbl_ws2.configure(text=f"Ticks:{ticks}  Next:{self.engine.next_candle}",
                text_color=ws_color)

            # Update rows
            total_pnl = 0.0
            for row in self._rows:
                # Sync qty
                st = row.st
                if not st.cfg.is_mcx and st.cfg.name in self._stock_qty_vars:
                    new_q = self._stock_qty_vars[st.cfg.name].get()
                    if new_q > 0: st.qty = new_q
                row.update()
                total_pnl += st.unrealised_pnl

            if self._nifty_row:
                self._nifty_row.update()
                total_pnl += self.nifty_st.pnl if self.nifty_st else 0

            pnl_color = GR if total_pnl > 0 else RD if total_pnl < 0 else GOLD
            self._lbl_pnl.configure(
                text=f"Net P&L: ₹{total_pnl:+,.2f}", text_color=pnl_color)

            # Summary
            long  = sum(1 for s in self.instruments if s.position=="LONG")
            short = sum(1 for s in self.instruments if s.position=="SHORT")
            flat  = sum(1 for s in self.instruments if s.position=="FLAT")
            self._lbl_summary.configure(
                text=f"  ↑LONG: {long}   ↓SHORT: {short}   FLAT: {flat}"
                     f"   |   Total P&L: ₹{total_pnl:+,.2f}")

        self.after(500, self._tick)


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════
def main():
    try:
        app = App()
        app.mainloop()
    except Exception as e:
        import traceback
        ctk.CTk().withdraw()
        import tkinter.messagebox as mb
        mb.showerror("Balfund HA Trader — Startup Error",
                     f"The application crashed on startup:\n\n{traceback.format_exc()}")

if __name__ == "__main__":
    main()
