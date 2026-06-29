"""
╔══════════════════════════════════════════════════════════════════════════╗
║  BALFUND TRADING PVT. LTD.                                                ║
║  Supertrend NIFTY + SENSEX ATM Options Trader  v1.0                      ║
║                                                                          ║
║  Signal     : Supertrend on HEIKIN-ASHI candles (Wilder RMA ATR)        ║
║  Logic      : ST GREEN → buy ATM CE  |  ST RED → buy ATM PE             ║
║  Indices    : NIFTY (sid 13) + SENSEX (sid 51) — independent signals    ║
║  Timeframes : 5s / 1m / 5m / 15m  (WS-driven candles)                   ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
from __future__ import annotations
import os, sys, time, json, struct, threading, queue, csv, io, re
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Tuple
from pathlib import Path
import requests, websocket
import customtkinter as ctk
import tkinter as tk
from tkinter import ttk

# ══════════════════════════════════════════════════════════════════════════════
#  PATHS & CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
BASE_DIR          = Path(sys.executable).parent if getattr(sys,"frozen",False) else Path(__file__).parent
CREDS_FILE        = BASE_DIR / "balfund_dhan_creds.json"
SETTINGS_FILE     = BASE_DIR / "balfund_st_settings.json"
MASTER_CACHE      = BASE_DIR / "dhan_master_cache.csv"

INTRADAY_URL  = "https://api.dhan.co/v2/charts/intraday"
OC_EXPIRY_URL = "https://api.dhan.co/v2/optionchain/expirylist"
OC_URL        = "https://api.dhan.co/v2/optionchain"
ORDER_URL     = "https://api.dhan.co/v2/orders"
MASTER_URL    = "https://images.dhan.co/api-data/api-scrip-master.csv"
WS_URL        = "wss://api-feed.dhan.co?version=2&token={tok}&clientId={cid}&authType=2"

# Index spot definitions (confirmed from DhanHQ SDK)
NIFTY  = {"name":"NIFTY",  "sid":"13", "seg":"IDX_I", "step":50,  "opt_seg":"NSE_FNO", "u_scrip":13, "u_seg":"IDX_I", "lot_default":65}
SENSEX = {"name":"SENSEX", "sid":"51", "seg":"IDX_I", "step":100, "opt_seg":"BSE_FNO", "u_scrip":51, "u_seg":"IDX_I", "lot_default":20}

# MCX futures definitions (security id + expiry resolved from master CSV at runtime)
# key = internal name, "match" = symbol prefixes to match in SEM_TRADING_SYMBOL
MCX_FUTURES = {
    # klass: bullion/base_metal (physically settled) or cash (Crude/NatGas).
    # roll_days: stop opening NEW positions when days-to-expiry <= this, and
    # auto-roll to the next contract. Buffers set 1 day before the tender period
    # (bullion tender = 5d before expiry, base-metal tender = 3d; cash = none).
    "SILVERMICRO": {"name":"SILVERMICRO","seg":"MCX_COMM","match":["SILVERMIC"], "klass":"bullion",    "roll_days":6},
    "ZINCMINI":    {"name":"ZINCMINI",   "seg":"MCX_COMM","match":["ZINCMINI"],  "klass":"base_metal", "roll_days":4},
    "NATGASMINI":  {"name":"NATGASMINI", "seg":"MCX_COMM","match":["NATGASMINI"],"klass":"cash",       "roll_days":1},
    "GOLDPETAL":   {"name":"GOLDPETAL",  "seg":"MCX_COMM","match":["GOLDPETAL"], "klass":"bullion",    "roll_days":6},
}

# WS packet
REQ_SUB         = 15
RESP_TICK       = 2
RESP_DISCONNECT = 50
SEG_MAP = {1:"NSE_EQ",2:"NSE_FNO",3:"BSE_EQ",4:"MCX_COMM",5:"CUR",7:"BSE_FNO",8:"MCX_COMM",9:"IDX_I"}

DISCONNECT_REASONS = {
    805:"Too many connections (max 5) — close other Dhan sessions",
    806:"Data APIs not subscribed — check Dhan plan",
    807:"Access token expired — generate new token",
    808:"Authentication failed — Client ID/Token invalid",
    809:"Access token invalid — regenerate",
    810:"Client ID invalid",
}

# ── Rate-limit governor (Dhan: data 5/sec, order 10/sec) ─────────────────────
class RateGate:
    """Token-bucket style gate to keep REST calls under Dhan limits."""
    def __init__(self, max_per_sec: float):
        self.min_gap = 1.0 / max_per_sec
        self._last = 0.0
        self._lock = threading.Lock()
    def wait(self):
        with self._lock:
            now = time.time()
            gap = now - self._last
            if gap < self.min_gap:
                time.sleep(self.min_gap - gap)
            self._last = time.time()

DATA_GATE  = RateGate(4.0)   # 4 req/sec for data (limit is 5, keep margin)
ORDER_GATE = RateGate(8.0)   # 8 req/sec for orders (limit is 10, keep margin)
OC_GATE    = RateGate(0.30)  # Option chain: 1 req per 3.3s (Dhan limit 1/3s)

# ══════════════════════════════════════════════════════════════════════════════
#  THEME
# ══════════════════════════════════════════════════════════════════════════════
BG="#FFFFFF"; CARD="#F0F4FA"; HBG="#1E3A5F"
AC="#1A56DB"; ACL="#3B82F6"; GOLD="#D4A017"
GR="#059669"; RD="#DC2626"; GY="#9CA3AF"
TX="#111827"; TD="#6B7280"; BD="#D1D5DB"
FONT=("Segoe UI",11,"bold")

# ══════════════════════════════════════════════════════════════════════════════
#  CANDLE AGGREGATOR
# ══════════════════════════════════════════════════════════════════════════════
class CandleAgg:
    def __init__(self, iv, seed=None):
        self.iv=iv; self._l=threading.Lock()
        self._c=list(seed or []); self._cur=None
    def on_tick(self, p, ts):
        b=int(ts//self.iv)*self.iv
        with self._l:
            if self._cur is None or self._cur["bucket"]!=b:
                if self._cur:
                    self._c.append(self._cur)
                    if len(self._c)>500: self._c=self._c[-500:]
                self._cur={"bucket":b,"open":p,"high":p,"low":p,"close":p}
            else:
                c=self._cur
                if p>c["high"]:c["high"]=p
                if p<c["low"]:c["low"]=p
                c["close"]=p
    def candles(self):
        with self._l: return list(self._c)
    def ready(self,n): 
        with self._l: return len(self._c)>=n

# ══════════════════════════════════════════════════════════════════════════════
#  HEIKIN-ASHI
# ══════════════════════════════════════════════════════════════════════════════
def heikin_ashi(candles: List[dict]) -> List[dict]:
    """Convert regular OHLC candles to Heikin-Ashi candles.
       HA_close = (O+H+L+C)/4
       HA_open  = (prev HA_open + prev HA_close)/2   (seed: (O+C)/2)
       HA_high  = max(H, HA_open, HA_close)
       HA_low   = min(L, HA_open, HA_close)
    """
    ha=[]
    for i,c in enumerate(candles):
        o,h,l,cl=c["open"],c["high"],c["low"],c["close"]
        ha_close=(o+h+l+cl)/4.0
        ha_open=(o+cl)/2.0 if i==0 else (ha[-1]["open"]+ha[-1]["close"])/2.0
        ha.append({"bucket":c["bucket"],"open":ha_open,
                   "high":max(h,ha_open,ha_close),
                   "low":min(l,ha_open,ha_close),"close":ha_close})
    return ha

# ══════════════════════════════════════════════════════════════════════════════
#  SUPERTREND  (ChartIQ spec — Wilder RMA ATR)
# ══════════════════════════════════════════════════════════════════════════════
def compute_supertrend(candles: List[dict], period: int, mult: float) -> List[dict]:
    """
    Supertrend — exact TradingView / Dhan logic (matches reference MQL5 EA).
      TR     = max(H-L, |H-prevC|, |L-prevC|)   (first bar = H-L)
      ATR    = RMA: ATR(i) = alpha*TR(i) + (1-alpha)*ATR(i-1),  alpha = 1/period
               (seeded with SMA of first `period` TRs)
      hl2    = (H+L)/2
      basicUpper = hl2 + factor*ATR ; basicLower = hl2 - factor*ATR
      upperBand(i) = basicUpper if basicUpper<prevUpper OR prevClose>prevUpper else prevUpper
      lowerBand(i) = basicLower if basicLower>prevLower OR prevClose<prevLower else prevLower
      direction:
        if prevDir==UP:   flip DOWN if close < lowerBand(i)  else stay UP
        if prevDir==DOWN: flip UP   if close > upperBand(i)  else stay DOWN
      ST line = lowerBand when UP (GREEN), upperBand when DOWN (RED)
    """
    n=len(candles)
    if n<period+1: return []
    alpha=1.0/period

    # True Range (index 0 has no previous close → H-L)
    tr=[0.0]*n
    tr[0]=candles[0]["high"]-candles[0]["low"]
    for i in range(1,n):
        h=candles[i]["high"]; l=candles[i]["low"]; pc=candles[i-1]["close"]
        tr[i]=max(h-l, abs(h-pc), abs(l-pc))

    # ATR via RMA, seeded with SMA of first `period` TRs at index (period-1)
    atr=[0.0]*n
    atr[period-1]=sum(tr[0:period])/period
    for i in range(period,n):
        atr[i]=alpha*tr[i]+(1.0-alpha)*atr[i-1]

    out=[]
    prev_upper=prev_lower=0.0
    prev_close=0.0
    prev_dir=1   # 1 = up (GREEN), -1 = down (RED)
    for i in range(period-1,n):
        h=candles[i]["high"]; l=candles[i]["low"]; c=candles[i]["close"]
        hl2=(h+l)/2.0
        bu=hl2+mult*atr[i]
        bl=hl2-mult*atr[i]
        if i==period-1:
            # First computed bar — initialize bands & direction
            upper=bu; lower=bl
            direction = 1 if c>=hl2 else -1
        else:
            upper = bu if (bu<prev_upper or prev_close>prev_upper) else prev_upper
            lower = bl if (bl>prev_lower or prev_close<prev_lower) else prev_lower
            if prev_dir==1:
                direction = -1 if c<lower else 1
            else:
                direction = 1 if c>upper else -1
        st = lower if direction==1 else upper
        out.append({"bucket":candles[i]["bucket"],"st":round(st,2),
                    "dir":"GREEN" if direction==1 else "RED","atr":round(atr[i],4)})
        prev_upper=upper; prev_lower=lower; prev_close=c; prev_dir=direction
    return out

# ══════════════════════════════════════════════════════════════════════════════
#  TOKEN
# ══════════════════════════════════════════════════════════════════════════════
def api_generate_token(client_id: str, pin: str, totp_secret: str) -> dict:
    """Generate a fresh access token via PIN + TOTP.
    POST https://auth.dhan.co/app/generateAccessToken?dhanClientId=&pin=&totp="""
    import pyotp
    totp_code = pyotp.TOTP(totp_secret.strip().replace(" ","")).now()
    url = "https://auth.dhan.co/app/generateAccessToken"
    params = {"dhanClientId": client_id, "pin": pin, "totp": totp_code}
    resp = requests.post(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "accessToken" in data:
        return {"success":True,"access_token":data["accessToken"],
                "expiry":data.get("expiryTime",""),"client_name":data.get("dhanClientName","")}
    return {"success":False,"error":data.get("errorMessage") or data.get("message") or str(data)}

def api_renew_token(client_id: str, access_token: str) -> dict:
    """Renew an existing valid token. GET https://api.dhan.co/v2/RenewToken"""
    url = "https://api.dhan.co/v2/RenewToken"
    headers = {"access-token":access_token,"dhanClientId":client_id,"Content-Type":"application/json"}
    resp = requests.get(url, headers=headers, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if "accessToken" in data:
        return {"success":True,"access_token":data["accessToken"],
                "expiry":data.get("expiryTime",""),"client_name":data.get("dhanClientName","")}
    return {"success":False,"error":data.get("errorMessage") or data.get("message") or str(data)}

def api_verify_token(client_id: str, access_token: str) -> bool:
    """Ping profile endpoint to check token validity."""
    if not access_token: return False
    try:
        resp = requests.get("https://api.dhan.co/v2/profile",
            headers={"access-token":access_token,"client-id":client_id},timeout=10)
        return resp.status_code==200
    except Exception:
        return False

# ══════════════════════════════════════════════════════════════════════════════
#  REST — rate-limited
# ══════════════════════════════════════════════════════════════════════════════
def fetch_ohlc(cid, tok, sid, seg, iv, days=5, instrument="INDEX"):
    DATA_GATE.wait()
    now=datetime.now()
    hdrs={"Content-Type":"application/json","access-token":tok,"client-id":cid}
    pl={"securityId":sid,"exchangeSegment":seg,"instrument":instrument,
        "expiryCode":0,"oi":False,"interval":iv,
        "fromDate":(now-timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),
        "toDate":now.strftime("%Y-%m-%d %H:%M:%S")}
    for att in range(4):
        r=requests.post(INTRADAY_URL,headers=hdrs,json=pl,timeout=20)
        if r.status_code==429:
            time.sleep(2*(att+1)); continue
        r.raise_for_status(); break
    d=r.json()
    opens=d.get("open",[]);highs=d.get("high",[]);lows=d.get("low",[]);closes=d.get("close",[])
    times=d.get("timestamp",d.get("start_Time",[]))
    out=[]
    for i in range(len(opens)):
        ts=times[i]
        if isinstance(ts,str):
            try:epoch=int(datetime.strptime(ts,"%Y-%m-%d %H:%M:%S").timestamp())
            except:epoch=0
        else:epoch=int(ts)
        out.append({"bucket":epoch,"open":float(opens[i]),"high":float(highs[i]),
                    "low":float(lows[i]),"close":float(closes[i])})
    return out

# ══════════════════════════════════════════════════════════════════════════════
#  MASTER CSV + MCX FUTURES RESOLVER
# ══════════════════════════════════════════════════════════════════════════════
def load_master(log_fn=print, force=False):
    if MASTER_CACHE.exists() and not force:
        mtime=MASTER_CACHE.stat().st_mtime
        age_h=(time.time()-mtime)/3600
        same_day=date.fromtimestamp(mtime)==date.today()
        # Use cache only if it's fresh AND from today (so expiries roll daily)
        if age_h<12 and same_day:
            log_fn(f"  Using cached master CSV ({age_h:.1f}h old, today)")
            raw=MASTER_CACHE.read_text(encoding="utf-8",errors="ignore")
            if raw.startswith("\ufeff"): raw=raw[1:]
            return list(csv.DictReader(io.StringIO(raw)))
    log_fn("  Downloading fresh master CSV...")
    r=requests.get(MASTER_URL,stream=True,timeout=120); r.raise_for_status()
    buf=io.BytesIO()
    for chunk in r.iter_content(65536): buf.write(chunk)
    raw=buf.getvalue().decode("utf-8",errors="ignore")
    if raw.startswith("\ufeff"): raw=raw[1:]
    MASTER_CACHE.write_text(raw,encoding="utf-8")
    return list(csv.DictReader(io.StringIO(raw)))

def build_index_option_lots(rows) -> Dict[str,int]:
    """Map underlying index name → lot size, read from its OPTIDX rows in the
       scrip master. Robust: all strikes/expiries of one index share one lot,
       so this never depends on matching a specific option's security_id
       (which differs between the option-chain API and the CSV for BSE)."""
    lots={}
    # Order matters: match more specific names before 'NIFTY'/'SENSEX'
    names=["MIDCPNIFTY","FINNIFTY","BANKNIFTY","NIFTYNXT50","NIFTY","SENSEX50","BANKEX","SENSEX"]
    for row in rows:
        instr=row.get("SEM_INSTRUMENT_NAME","").strip().upper()
        if instr!="OPTIDX": continue
        lot=row.get("SEM_LOT_UNITS","").strip()
        try: lot_i=int(float(lot))
        except: continue
        if lot_i<=0: continue
        ts=row.get("SEM_TRADING_SYMBOL","").strip().upper()
        cs=row.get("SEM_CUSTOM_SYMBOL","").strip().upper()
        for nm in names:
            if ts.startswith(nm) or cs.startswith(nm):
                lots.setdefault(nm, lot_i)
                break
    return lots

def resolve_mcx_future(rows, fut, log_fn=print) -> Optional[dict]:
    """Resolve the active (nearest non-expired) MCX futures contract.
       `fut` is an entry from MCX_FUTURES with 'name' and 'match' prefixes.
       Auto-rolls: once a contract's expiry date passes, the next month is
       picked automatically on the next resolve."""
    today=date.today()
    MONTHS={"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
            "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    name=fut["name"]; prefixes=[p.upper() for p in fut["match"]]
    found=[]
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID","").strip().upper()!="MCX": continue
        ts=row.get("SEM_TRADING_SYMBOL","").strip().upper()
        if not ts.endswith("-FUT"): continue
        # Match the commodity base (before the first '-') — exact match only
        base=ts.split("-")[0]
        if base not in prefixes: continue
        m=re.search(r"(\d{2})([A-Z]{3})(\d{4})",ts)
        if not m: continue
        try: exp=date(int(m.group(3)),MONTHS[m.group(2)],int(m.group(1)))
        except: continue
        sid=row.get("SEM_SMST_SECURITY_ID","").strip()
        lot=row.get("SEM_LOT_UNITS","").strip()
        if not sid: continue
        found.append((exp,sid,ts,lot,base))
    if not found:
        log_fn(f"  ✗ {name}: no futures contract found in master CSV"); return None
    # Candidates with expiry today or later, sorted nearest first
    cands=sorted([x for x in found if x[0]>=today],key=lambda x:x[0])
    if not cands:
        log_fn(f"  ✗ {name}: all contracts expired — master CSV may be stale"); return None
    roll_days=int(fut.get("roll_days",1))
    klass=fut.get("klass","cash")
    # Prefer the nearest contract that is SAFELY tradeable (outside tender buffer)
    safe=[x for x in cands if (x[0]-today).days > roll_days]
    chosen=None; rolled=False
    if safe:
        chosen=safe[0]
        # Did we skip the front month because it's in/near tender?
        if chosen is not cands[0]:
            rolled=True
            near=cands[0]; nd=(near[0]-today).days
            log_fn(f"  ↻ {name}: front month {near[2]} ({nd}d left) inside "
                   f"{klass} tender buffer ({roll_days}d) — rolling to next")
    else:
        # No safe contract (next month not listed yet) — use nearest, warn
        chosen=cands[0]
        nd=(chosen[0]-today).days
        log_fn(f"  ⚠ {name}: only {chosen[2]} ({nd}d left) available, within "
               f"{klass} tender buffer ({roll_days}d) — new positions may be blocked")
    e,sid,ts,lot,base=chosen
    try: lot_i=int(float(lot))
    except: lot_i=1
    days_left=(e-today).days
    tag="↻ rolled" if rolled else "✓"
    log_fn(f"  {tag} {name} sid={sid}  {ts}  expiry={e} ({days_left}d left)  "
           f"lot={lot_i}  [{klass}, roll@{roll_days}d]")
    return {"sid":sid,"ts":ts,"expiry":str(e),"lot":lot_i,"name":name,"days_left":days_left}

# ══════════════════════════════════════════════════════════════════════════════
#  OPTION CHAIN  (cached + rate-limited)
# ══════════════════════════════════════════════════════════════════════════════
class OptionResolver:
    """Resolves ATM CE/PE security IDs. Caches expiry list and chain.
       Lot size is read from the scrip master per underlying index (OPTIDX rows),
       so it's always current, from the API, and never hardcoded."""
    def __init__(self, cid, tok, log_fn, index_lots=None):
        self.cid=cid; self.tok=tok; self.log=log_fn
        self._exp_cache: Dict[str,Tuple[float,list]] = {}      # idx_name → (ts, [expiries])
        self._chain_cache: Dict[str,Tuple[float,dict]] = {}    # f"{idx}:{expiry}" → (ts, oc)
        self._index_lots: Dict[str,int] = index_lots or {}     # underlying → lot_size
        self._diag_done: set = set()                           # names already diagnosed
        self._lock=threading.Lock()

    def set_index_lots(self, index_lots: Dict[str,int]):
        self._index_lots = index_lots or {}

    def _lot_for(self, idx) -> int:
        """Lot size from scrip master, by underlying index name."""
        lot=self._index_lots.get(idx["name"].upper())
        if lot and lot>0:
            return lot
        return int(idx.get("lot_default",0))   # final safety net

    def _hdrs(self):
        return {"Content-Type":"application/json","access-token":self.tok,"client-id":self.cid}

    def expiries(self, idx) -> list:
        with self._lock:
            c=self._exp_cache.get(idx["name"])
            if c and (time.time()-c[0])<300:   # 5-min cache
                return c[1]
        OC_GATE.wait()
        r=requests.post(OC_EXPIRY_URL,headers=self._hdrs(),
            json={"UnderlyingScrip":idx["u_scrip"],"UnderlyingSeg":idx["u_seg"]},timeout=10)
        r.raise_for_status()
        d=r.json()
        exps=sorted(d.get("data",d.get("expiryList",[])))[:3]
        with self._lock:
            self._exp_cache[idx["name"]]=(time.time(),exps)
        return exps

    def chain(self, idx, expiry) -> dict:
        key=f"{idx['name']}:{expiry}"
        with self._lock:
            c=self._chain_cache.get(key)
            if c and (time.time()-c[0])<30:   # 30s cache to avoid hammering
                return c[1]
        OC_GATE.wait()
        r=requests.post(OC_URL,headers=self._hdrs(),
            json={"UnderlyingScrip":idx["u_scrip"],"UnderlyingSeg":idx["u_seg"],
                  "Expiry":expiry},timeout=10)
        r.raise_for_status()
        d=r.json()
        with self._lock:
            self._chain_cache[key]=(time.time(),d)
        return d

    def atm_strike(self, spot, step):
        return int(round(spot/step))*step

    def resolve(self, idx, spot, opt_type) -> Optional[dict]:
        """Returns {sid, sym, ltp, lot, strike} for ATM CE or PE.
           Finds the nearest available strike in the chain (robust to key
           format / strike-interval differences between NSE and BSE)."""
        try:
            exps=self.expiries(idx)
            if not exps: return None
            expiry=exps[0]
            atm=self.atm_strike(spot,idx["step"])
            d=self.chain(idx,expiry)
            data=d.get("data",{})
            oc=data.get("oc",{})
            if not oc:
                self.log(f"[{idx['name']}] Empty option chain for {expiry}")
                return None
            # One-time diagnostic per index: show what the chain actually contains
            if idx["name"] not in self._diag_done:
                self._diag_done.add(idx["name"])
                sample=list(oc.keys())[:3]
                self.log(f"[{idx['name']}] chain spot(data.last_price)={data.get('last_price')} "
                         f"strikes={len(oc)} sample_keys={sample}")
            side_key="ce" if opt_type=="CE" else "pe"
            # Pick nearest strike that has the requested side with a valid security_id
            best=None; best_diff=None
            for k,node in oc.items():
                try: ks=float(k)
                except: continue
                sd=node.get(side_key) or {}
                if not sd.get("security_id"): continue
                diff=abs(ks-atm)
                if best_diff is None or diff<best_diff:
                    best_diff=diff; best=(ks,sd)
            if not best:
                self.log(f"[{idx['name']}] No {opt_type} strike with security_id near {atm}")
                return None
            strike,side=best
            sid=str(side.get("security_id",""))
            ltp=float(side.get("last_price",0) or 0)
            # If premium is 0/stale, fall back to average_price then bid/ask mid
            if ltp<=0:
                ltp=float(side.get("average_price",0) or 0)
            if ltp<=0:
                bid=float(side.get("top_bid_price",0) or 0); ask=float(side.get("top_ask_price",0) or 0)
                if bid>0 and ask>0: ltp=round((bid+ask)/2,2)
            # Lot size from scrip master per underlying (from API, never hardcoded)
            lot=self._lot_for(idx)
            if lot<=0:
                self.log(f"[{idx['name']}] ⚠ Lot size unavailable — order skipped")
                return None
            strike_i=int(strike) if float(strike).is_integer() else strike
            datestr=expiry.replace("-","")
            sym=f"{idx['name']}{datestr}{strike_i}{opt_type}"
            self.log(f"[{idx['name']}] ATM {opt_type} {strike_i} sid={sid} lot={lot} ltp={ltp}")
            return {"sid":sid,"sym":sym,"ltp":ltp,"lot":lot,"strike":strike_i,"expiry":expiry}
        except Exception as e:
            self.log(f"[{idx['name']}] Option resolve error: {e}")
            return None

# ══════════════════════════════════════════════════════════════════════════════
#  ORDER  (rate-limited)
# ══════════════════════════════════════════════════════════════════════════════
def _poll_order(cid, tok, order_id, log_fn, tries=8):
    """Poll GET /v2/orders/{id} until the order reaches a terminal state.
       Returns (status, traded_price). Market orders normally fill in <2s."""
    hdrs={"access-token":tok,"client-id":cid}
    url=f"{ORDER_URL}/{order_id}"
    last_status="TRANSIT"
    for i in range(tries):
        time.sleep(0.4)
        try:
            ORDER_GATE.wait()
            r=requests.get(url,headers=hdrs,timeout=10)
            if r.status_code!=200: continue
            d=r.json()
            if isinstance(d,list): d=d[0] if d else {}
            status=str(d.get("orderStatus","")).upper()
            price=float(d.get("averageTradedPrice",0) or d.get("price",0) or 0)
            last_status=status or last_status
            if status=="TRADED":
                return "TRADED", price
            if status in ("REJECTED","CANCELLED","EXPIRED"):
                return status, 0.0
        except Exception:
            continue
    return last_status, 0.0   # still TRANSIT/PENDING after polling = not filled

def place_order(cid, tok, seg, sid, side, qty, paper, log_fn, ref="") -> dict:
    """Place a MARKET order and CONFIRM it actually filled.
       Returns {"filled":bool, "price":float, "status":str, "order_id":str}.
       A position must only be considered open when filled is True."""
    tag=f"{ref} " if ref else ""
    if paper:
        log_fn(f"[PAPER] {tag}{side} qty={qty} {seg}:{sid}")
        return {"filled":True,"price":0.0,"status":"PAPER","order_id":""}
    ORDER_GATE.wait()
    hdrs={"Content-Type":"application/json","access-token":tok,"client-id":cid}
    pl={"dhanClientId":str(cid),"transactionType":side,"exchangeSegment":seg,
        "productType":"INTRADAY","orderType":"MARKET","validity":"DAY",
        "securityId":str(sid),"quantity":int(qty),
        "disclosedQuantity":0,"price":0,"triggerPrice":0,"afterMarketOrder":False}
    last_err=""
    for att in range(3):
        try:
            r=requests.post(ORDER_URL,headers=hdrs,json=pl,timeout=15)
        except Exception as e:
            last_err=f"network: {e}"; time.sleep(1.0); continue
        if r.status_code==429:
            last_err="429 rate limit"; time.sleep(1.5*(att+1)); continue
        if r.status_code!=200:
            try: body=r.json()
            except: body=r.text[:200]
            last_err=f"HTTP {r.status_code}: {body}"
            log_fn(f"[ORDER REJECTED] {tag}{side} {seg}:{sid} qty={qty} → {last_err}")
            return {"filled":False,"price":0.0,"status":"HTTP_ERR","order_id":""}
        try: d=r.json()
        except:
            log_fn(f"[ORDER ERR] {tag}{side} {seg}:{sid}: bad JSON"); 
            return {"filled":False,"price":0.0,"status":"BAD_JSON","order_id":""}
        status=str(d.get("orderStatus","")).upper()
        oid=str(d.get("orderId","") or "")
        # Immediate rejection
        if status in ("REJECTED","CANCELLED"):
            err=d.get("omsErrorDescription") or d.get("errorMessage") or "rejected by broker"
            log_fn(f"[ORDER REJECTED] {tag}{side} {seg}:{sid} qty={qty} id={oid} → {err}")
            return {"filled":False,"price":0.0,"status":status,"order_id":oid}
        # Accepted by Dhan — but NOT yet filled. Confirm execution.
        log_fn(f"[ORDER SENT] {tag}{side} {seg}:{sid} qty={qty} id={oid} status={status} — confirming...")
        price=float(d.get("averageTradedPrice",0) or 0)
        if status=="TRADED" and price>0:
            log_fn(f"[ORDER FILLED] {tag}{side} {seg}:{sid} qty={qty} @{price:.2f}")
            return {"filled":True,"price":price,"status":"TRADED","order_id":oid}
        if oid:
            fstatus,fprice=_poll_order(cid,tok,oid,log_fn)
            if fstatus=="TRADED":
                log_fn(f"[ORDER FILLED] {tag}{side} {seg}:{sid} qty={qty} @{fprice:.2f}")
                return {"filled":True,"price":fprice,"status":"TRADED","order_id":oid}
            # Did not fill — tell the user clearly (this is the ZINCMINI case)
            log_fn(f"[ORDER NOT FILLED] {tag}{side} {seg}:{sid} qty={qty} id={oid} "
                   f"stuck at {fstatus} — order did NOT go through "
                   f"(near-expiry close-only or no liquidity?)")
            return {"filled":False,"price":0.0,"status":fstatus,"order_id":oid}
        return {"filled":False,"price":0.0,"status":status,"order_id":oid}
    log_fn(f"[ORDER FAILED] {tag}{side} {seg}:{sid} qty={qty} after retries → {last_err}")
    return {"filled":False,"price":0.0,"status":"FAILED","order_id":""}

# ══════════════════════════════════════════════════════════════════════════════
#  WEBSOCKET FEED  (improved connection logic)
# ══════════════════════════════════════════════════════════════════════════════
class Feed:
    def __init__(self, cid, tok, on_tick, on_status):
        self.cid=cid; self.tok=tok
        self._on_tick=on_tick; self._on_status=on_status
        self._ws=None; self._stop=threading.Event()
        self._subs: List[Tuple[str,str]] = []   # (sid, seg)
        self._sub_lock=threading.Lock()
        self.connected=False; self.ticks=0
        self._logged_once=False; self._fatal=False
        self._opened_at=0.0; self._pkts_open=0; self._instant=0

    def start(self): threading.Thread(target=self._run,daemon=True).start()
    def stop(self):
        self._stop.set()
        if self._ws:
            try:self._ws.close()
            except:pass

    def add_sub(self, sid, seg):
        with self._sub_lock:
            if (sid,seg) not in self._subs: self._subs.append((sid,seg))
        if self._ws and self.connected:
            try:self._ws.send(json.dumps({"RequestCode":REQ_SUB,"InstrumentCount":1,
                "InstrumentList":[{"ExchangeSegment":seg,"SecurityId":sid}]}))
            except:pass

    def _run(self):
        delay=3
        while not self._stop.is_set() and not self._fatal:
            url=WS_URL.format(tok=self.tok,cid=self.cid)
            self._ws=websocket.WebSocketApp(url,on_open=self._o,on_message=self._m,
                on_error=self._e,on_close=self._c)
            self._ws.run_forever(ping_interval=20,ping_timeout=10)
            self.connected=False
            if self._stop.is_set() or self._fatal: break
            self._on_status(f"Reconnecting in {delay}s...")
            time.sleep(delay); delay=min(delay*2,60)

    def _o(self, ws):
        self.connected=True; self._opened_at=time.time(); self._pkts_open=self.ticks; self._instant=0
        with self._sub_lock: subs=list(self._subs)
        for i in range(0,len(subs),100):
            batch=subs[i:i+100]
            ws.send(json.dumps({"RequestCode":REQ_SUB,"InstrumentCount":len(batch),
                "InstrumentList":[{"ExchangeSegment":seg,"SecurityId":sid} for sid,seg in batch]}))
            if i+100<len(subs): time.sleep(0.1)
        if not self._logged_once:
            self._on_status(f"Connected — {len(subs)} instruments"); self._logged_once=True
        else:
            self._on_status(f"Reconnected — {len(subs)} instruments")

    def _m(self, ws, raw):
        try:
            b=bytes(raw)
            if len(b)<8: return
            code=b[0]; sid=str(struct.unpack_from("<I",b,4)[0])
            if code==RESP_DISCONNECT:
                if len(b)>=10:
                    rc=int(struct.unpack_from("<H",b,8)[0])
                    desc=DISCONNECT_REASONS.get(rc,"Unknown")
                    self._on_status(f"Dhan disconnect {rc}: {desc}")
                    if rc in (806,807,808,809,810): self._fatal=True
                return
            if code!=RESP_TICK: return
            if len(b)<12: return
            ltp=float(struct.unpack_from("<f",b,8)[0])
            if ltp<=0: return
            self.ticks+=1
            self._on_tick(sid, ltp)
        except: pass

    def _e(self, ws, err):
        self.connected=False
        if "429" in str(err): self._on_status("429 — IP not whitelisted or token in use")
        else: self._on_status(f"Error: {err}")

    def _c(self, ws, *a):
        was=self.connected; self.connected=False
        life=time.time()-self._opened_at if self._opened_at else 999
        if was and life<5 and (self.ticks-self._pkts_open)==0:
            self._instant+=1
            self._on_status(f"Instant disconnect #{self._instant}")
            if self._instant>=5:
                self._on_status("5 instant disconnects — stopping"); self._fatal=True

# ══════════════════════════════════════════════════════════════════════════════
#  INDEX TRADER  (one per NIFTY / SENSEX)
# ══════════════════════════════════════════════════════════════════════════════
class IndexTrader:
    def __init__(self, idx, lots, period, mult, paper, resolver, feed,
                 cid, tok, log_fn):
        self.idx=idx; self.lots=lots; self.period=period; self.mult=mult
        self.paper=paper; self.resolver=resolver; self.feed=feed
        self.cid=cid; self.tok=tok; self.log=log_fn
        # State
        self.spot=0.0
        self.st_value=0.0; self.st_dir="-"; self.atr=0.0
        self.position="FLAT"          # FLAT / CE / PE
        self.opt_sid=""; self.opt_sym=""; self.entry=0.0; self.opt_ltp=0.0
        self.strike=0; self.lot_size=0
        self.bar_time="-"; self.status="Waiting"
        self._startup_done=False
        self._lock=threading.Lock()

    @property
    def pnl(self):
        if self.position=="FLAT" or self.entry==0: return 0.0
        return round((self.opt_ltp-self.entry)*self.lots*self.lot_size,2)

    def on_spot_tick(self, ltp):
        with self._lock: self.spot=round(ltp,2)

    def on_opt_tick(self, sid, ltp):
        if sid==self.opt_sid:
            with self._lock: self.opt_ltp=round(ltp,2)

    def process(self, candles, startup):
        ha=heikin_ashi(candles)
        sts=compute_supertrend(ha, self.period, self.mult)
        if not sts: return
        last=sts[-1]
        with self._lock:
            self.st_value=last["st"]; self.st_dir=last["dir"]; self.atr=last["atr"]
            self.bar_time=datetime.fromtimestamp(last["bucket"]).strftime("%H:%M:%S")
        new_dir=last["dir"]
        spot=self.spot or candles[-1]["close"]

        # Startup: enter according to current direction
        if startup and not self._startup_done:
            self._startup_done=True
            want="CE" if new_dir=="GREEN" else "PE"
            self.log(f"[{self.idx['name']}] Startup → ST={new_dir} → buy {want}")
            self._enter(want, spot); return
        if startup: return

        # Running: act on direction flip
        want="CE" if new_dir=="GREEN" else "PE"
        if self.position!=want:
            self.log(f"[{self.idx['name']}] ST flipped {new_dir} → switch to {want}")
            if self.position!="FLAT": self._exit()
            time.sleep(0.15)
            self._enter(want, spot)

    def _enter(self, opt_type, spot):
        info=self.resolver.resolve(self.idx, spot, opt_type)
        if not info:
            self.log(f"[{self.idx['name']}] Could not resolve ATM {opt_type}"); return
        qty=self.lots*info["lot"]
        res=place_order(self.cid,self.tok,self.idx["opt_seg"],info["sid"],
                        "BUY",qty,self.paper,self.log,ref=f"[{self.idx['name']}]")
        if not res["filled"]:
            self.log(f"[{self.idx['name']}] ENTRY FAILED ({res['status']}) — staying FLAT")
            with self._lock: self.status=f"ENTRY FAILED:{res['status']}"
            return
        with self._lock:
            self.position=opt_type; self.opt_sid=info["sid"]; self.opt_sym=info["sym"]
            self.entry=res["price"] or info["ltp"]; self.opt_ltp=info["ltp"]
            self.strike=info["strike"]; self.lot_size=info["lot"]
            self.status=f"{opt_type} {info['strike']}"
        self.feed.add_sub(info["sid"], self.idx["opt_seg"])
        self.log(f"[{self.idx['name']}] OPEN {info['sym']} qty={qty} @{self.entry:.2f}")

    def _exit(self):
        if self.position=="FLAT": return
        pnl=self.pnl
        qty=self.lots*self.lot_size
        res=place_order(self.cid,self.tok,self.idx["opt_seg"],self.opt_sid,
                        "SELL",qty,self.paper,self.log,ref=f"[{self.idx['name']}]")
        if not res["filled"]:
            self.log(f"[{self.idx['name']}] ⚠ EXIT FAILED ({res['status']}) — "
                     f"position STILL OPEN, will retry next signal")
            return
        self.log(f"[{self.idx['name']}] EXIT {self.opt_sym} @{res['price']:.2f}  P&L=₹{pnl:+.2f}")
        with self._lock:
            self.position="FLAT"; self.entry=0.0; self.opt_sid=""
            self.opt_sym=""; self.status="FLAT"

    def square_off(self):
        if self.position!="FLAT": self._exit()

# ══════════════════════════════════════════════════════════════════════════════
#  FUTURE TRADER  (SILVERMICRO — long/short reversal on its own chart)
# ══════════════════════════════════════════════════════════════════════════════
class FutureTrader:
    def __init__(self, fut, sid, lot_size, lots, period, mult, paper, feed,
                 cid, tok, log_fn):
        self.fut=fut; self.name=fut["name"]; self.sid=sid; self.seg=fut["seg"]
        self.lot_size=lot_size; self.lots=lots
        self.period=period; self.mult=mult; self.paper=paper
        self.feed=feed; self.cid=cid; self.tok=tok; self.log=log_fn
        self.ltp=0.0
        self.st_value=0.0; self.st_dir="-"; self.atr=0.0
        self.position="FLAT"          # FLAT / LONG / SHORT
        self.entry=0.0; self.bar_time="-"; self.status="Waiting"; self.strike=0
        self.expiry="-"
        self._startup_done=False; self._lock=threading.Lock()

    @property
    def pnl(self):
        if self.position=="FLAT" or self.entry==0: return 0.0
        d=(self.ltp-self.entry) if self.position=="LONG" else (self.entry-self.ltp)
        return round(d*self.lots*self.lot_size,2)

    def on_tick(self, sid, ltp):
        if sid==self.sid:
            with self._lock: self.ltp=round(ltp,2)

    def process(self, candles, startup):
        ha=heikin_ashi(candles)
        sts=compute_supertrend(ha, self.period, self.mult)
        if not sts: return
        last=sts[-1]
        with self._lock:
            self.st_value=last["st"]; self.st_dir=last["dir"]; self.atr=last["atr"]
            self.bar_time=datetime.fromtimestamp(last["bucket"]).strftime("%H:%M:%S")
        new_dir=last["dir"]
        want="LONG" if new_dir=="GREEN" else "SHORT"

        if startup and not self._startup_done:
            self._startup_done=True
            self.log(f"[{self.name}] Startup → ST={new_dir} → go {want}")
            self._go(want); return
        if startup: return

        if self.position!=want:
            self.log(f"[{self.name}] ST flipped {new_dir} → reverse to {want}")
            if self.position!="FLAT": self._close()
            time.sleep(0.15)
            self._go(want)

    def _go(self, direction):
        side="BUY" if direction=="LONG" else "SELL"
        qty=self.lots*self.lot_size
        res=place_order(self.cid,self.tok,self.seg,self.sid,side,qty,self.paper,self.log,
                        ref=f"[{self.name}]")
        if not res["filled"]:
            self.log(f"[{self.name}] ENTRY FAILED ({res['status']}) — staying FLAT")
            with self._lock: self.status=f"ENTRY FAILED:{res['status']}"
            return
        with self._lock:
            self.position=direction; self.entry=res["price"] or self.ltp
            self.status=f"{'↑LONG' if direction=='LONG' else '↓SHORT'}"
        self.log(f"[{self.name}] OPEN {side} {qty} @{self.entry:.2f}  ({direction})")

    def _close(self):
        if self.position=="FLAT": return
        side="SELL" if self.position=="LONG" else "BUY"
        qty=self.lots*self.lot_size; pnl=self.pnl
        res=place_order(self.cid,self.tok,self.seg,self.sid,side,qty,self.paper,self.log,
                        ref=f"[{self.name}]")
        if not res["filled"]:
            self.log(f"[{self.name}] ⚠ CLOSE FAILED ({res['status']}) — "
                     f"position STILL OPEN, will retry next signal")
            return
        self.log(f"[{self.name}] CLOSE {side} {qty} @{res['price']:.2f}  P&L=₹{pnl:+.2f}")
        with self._lock:
            self.position="FLAT"; self.entry=0.0; self.status="FLAT"

    def square_off(self):
        if self.position!="FLAT": self._close()

# ══════════════════════════════════════════════════════════════════════════════
#  ENGINE
# ══════════════════════════════════════════════════════════════════════════════
class Engine:
    def __init__(self, cid, tok, iv_sec, period, mult, paper,
                 nifty_lots, sensex_lots, sq_time, trade_nifty, trade_sensex,
                 mcx_selections, log_fn):
        # mcx_selections: dict name → lots, e.g. {"SILVERMICRO":1, "ZINCMINI":2}
        self.cid=cid; self.tok=tok; self.iv_sec=iv_sec
        self.period=period; self.mult=mult; self.paper=paper
        self.sq_time=sq_time; self.log=log_fn
        self._stop=threading.Event()
        self.ws_status="Not started"; self.next_candle="-"; self.sq_done=False

        self.resolver=OptionResolver(cid,tok,log_fn)
        self.feed=Feed(cid,tok,self._on_tick,self._on_status)

        # Index option traders
        self.traders: List[IndexTrader]=[]
        if trade_nifty:
            self.traders.append(IndexTrader(NIFTY,nifty_lots,period,mult,paper,
                self.resolver,self.feed,cid,tok,log_fn))
        if trade_sensex:
            self.traders.append(IndexTrader(SENSEX,sensex_lots,period,mult,paper,
                self.resolver,self.feed,cid,tok,log_fn))

        # Spot aggregators + sid → trader map
        self._aggs: Dict[str,CandleAgg]={}
        self._spot_map: Dict[str,IndexTrader]={}
        for t in self.traders:
            self._aggs[t.idx["sid"]]=CandleAgg(iv_sec)
            self._spot_map[t.idx["sid"]]=t
            self.feed.add_sub(t.idx["sid"], t.idx["seg"])

        # Future traders (resolved later in _run after master CSV loads)
        self._mcx_selections={k:v for k,v in (mcx_selections or {}).items() if v and v>0}
        self.futures: List[FutureTrader]=[]
        self._fut_map: Dict[str,FutureTrader]={}

    def start(self): threading.Thread(target=self._run,daemon=True).start()
    def stop(self): self._stop.set(); self.feed.stop()
    def sq_all(self):
        for t in self.traders: t.square_off()
        for f in self.futures: f.square_off()

    def _on_status(self, msg):
        self.ws_status=msg; self.log(f"[WS] {msg}")

    def _on_tick(self, sid, ltp):
        now=time.time()
        # Index spot tick?
        t=self._spot_map.get(sid)
        if t:
            t.on_spot_tick(ltp)
            agg=self._aggs.get(sid)
            if agg: agg.on_tick(ltp, now)
            return
        # Future tick? (signal source = traded instrument)
        f=self._fut_map.get(sid)
        if f:
            f.on_tick(sid, ltp)
            agg=self._aggs.get(sid)
            if agg: agg.on_tick(ltp, now)
            return
        # Option tick → route to all index traders
        for tr in self.traders:
            tr.on_opt_tick(sid, ltp)

    def _run(self):
        rest_iv="1" if self.iv_sec<=60 else "5" if self.iv_sec<=300 else "15"
        # Load master CSV once if we trade options (for lot sizes) or any MCX future
        rows=None
        need_master = bool(self.traders) or bool(self._mcx_selections)
        if need_master:
            try:
                rows=load_master(self.log)
            except Exception as e:
                self.log(f"[Master CSV ERR] {e}")
        # Build per-underlying index option lot map and hand to resolver
        if self.traders and rows:
            try:
                index_lots=build_index_option_lots(rows)
                self.resolver.set_index_lots(index_lots)
                shown={k:index_lots[k] for k in ("NIFTY","SENSEX") if k in index_lots}
                self.log(f"[Engine] Index option lots from scrip master: {shown or index_lots}")
            except Exception as e:
                self.log(f"[Lot map ERR] {e}")
        # Resolve each selected MCX future (auto-rolls to active expiry)
        if self._mcx_selections and rows:
            for name, lots in self._mcx_selections.items():
                fut=MCX_FUTURES.get(name)
                if not fut: continue
                try:
                    info=resolve_mcx_future(rows, fut, self.log)
                    if info:
                        ft=FutureTrader(fut,info["sid"],info["lot"],lots,
                            self.period,self.mult,self.paper,self.feed,
                            self.cid,self.tok,self.log)
                        ft.expiry=info["expiry"]
                        self.futures.append(ft)
                        self._fut_map[info["sid"]]=ft
                        self._aggs[info["sid"]]=CandleAgg(self.iv_sec)
                        self.feed.add_sub(info["sid"], fut["seg"])
                except Exception as e:
                    self.log(f"[{name} resolve ERR] {e}")

        self.log(f"[Engine] Seeding {len(self.traders)} indices + {len(self.futures)} futures...")
        # Seed index spots
        for t in self.traders:
            try:
                c=fetch_ohlc(self.cid,self.tok,t.idx["sid"],t.idx["seg"],rest_iv,instrument="INDEX")
                self._aggs[t.idx["sid"]]=CandleAgg(self.iv_sec,c[-200:])
                if c: t.spot=float(c[-1]["close"])
                self.log(f"[seed] {t.idx['name']} {len(c)} bars  spot={t.spot:.2f}")
            except Exception as e:
                self.log(f"[seed ERR] {t.idx['name']}: {e}")
        # Seed futures
        for f in self.futures:
            try:
                c=fetch_ohlc(self.cid,self.tok,f.sid,f.seg,rest_iv,instrument="FUTCOM")
                self._aggs[f.sid]=CandleAgg(self.iv_sec,c[-200:])
                if c: f.ltp=float(c[-1]["close"])
                self.log(f"[seed] {f.name} {len(c)} bars  ltp={f.ltp:.2f}")
            except Exception as e:
                self.log(f"[seed ERR] {f.name}: {e}")
        self.log("[Engine] Seed done — starting WebSocket")
        self.feed.start(); time.sleep(2)

        self._process(startup=True); self._set_next()
        while not self._stop.is_set():
            self._wait(); 
            if self._stop.is_set(): break
            self._check_sq()
            self._process(startup=False); self._set_next()
        self.log("[Engine] Stopped")

    def _set_next(self):
        n=(int(time.time())//self.iv_sec+1)*self.iv_sec
        self.next_candle=datetime.fromtimestamp(n).strftime("%H:%M:%S")
    def _wait(self):
        n=(int(time.time())//self.iv_sec+1)*self.iv_sec
        while time.time()<n and not self._stop.is_set(): time.sleep(0.05)
    def _check_sq(self):
        if self.sq_done: return
        if datetime.now().strftime("%H:%M")>=self.sq_time:
            self.log("[SqOff] Square-off time reached")
            self.sq_all(); self.sq_done=True

    def _process(self, startup):
        if self.sq_done:   # after square-off, no new entries for the day
            return
        for t in self.traders:
            if self._stop.is_set(): break
            agg=self._aggs.get(t.idx["sid"])
            if not agg or not agg.ready(self.period+2): 
                continue
            candles=agg.candles()
            t.process(candles, startup)
            time.sleep(0.15)   # gap between index order bursts
        for f in self.futures:
            if self._stop.is_set(): break
            agg=self._aggs.get(f.sid)
            if not agg or not agg.ready(self.period+2):
                continue
            candles=agg.candles()
            f.process(candles, startup)
            time.sleep(0.15)

# ══════════════════════════════════════════════════════════════════════════════
#  GUI
# ══════════════════════════════════════════════════════════════════════════════
ctk.set_appearance_mode("light"); ctk.set_default_color_theme("blue")

TREE_COLS=["Index","Spot","SuperT","Dir","ATR","Pos","Strike","Entry","LTP","P&L","Bar","Status"]
COL_W=[80,90,90,70,70,60,70,80,80,90,80,140]

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund — HA Supertrend  NIFTY+SENSEX+MCX  v1.1")
        self.geometry("1300x720"); self.configure(fg_color=BG)
        self.engine: Optional[Engine]=None
        self._running=False
        self._log_q: queue.Queue=queue.Queue()
        # In-app Dhan token state
        self._access_token=""
        self._token_client=""
        self._token_ok=False

        _s=self._load()
        _c=self._load_creds()
        self.client_id_var=ctk.StringVar(value=_c.get("client_id",""))
        self.pin_var=ctk.StringVar(value=_c.get("pin",""))
        self.totp_var=ctk.StringVar(value=_c.get("totp",""))
        self.save_creds_var=ctk.BooleanVar(value=_c.get("save",True))
        self.tf_var=ctk.StringVar(value=_s.get("tf","5s"))
        self.period_var=ctk.IntVar(value=_s.get("period",2))
        self.mult_var=ctk.DoubleVar(value=_s.get("mult",2.0))
        self.paper_var=ctk.BooleanVar(value=_s.get("paper",True))
        self.nifty_lots_var=ctk.IntVar(value=_s.get("nifty_lots",1))
        self.sensex_lots_var=ctk.IntVar(value=_s.get("sensex_lots",1))
        self.trade_nifty_var=ctk.BooleanVar(value=_s.get("trade_nifty",True))
        self.trade_sensex_var=ctk.BooleanVar(value=_s.get("trade_sensex",True))
        # MCX futures toggles + lots
        self.mcx_vars={}   # name → (BooleanVar trade, IntVar lots)
        _mcx_saved=_s.get("mcx",{})
        for _name in MCX_FUTURES:
            tv=ctk.BooleanVar(value=_mcx_saved.get(_name,{}).get("trade",False))
            lv=ctk.IntVar(value=_mcx_saved.get(_name,{}).get("lots",1))
            self.mcx_vars[_name]=(tv,lv)
        self.sq_var=ctk.StringVar(value=_s.get("sq","15:15"))

        self._build()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.after(400, self._tick)

    # ── Persistence ──
    def _load(self):
        try:
            if SETTINGS_FILE.exists(): return json.loads(SETTINGS_FILE.read_text())
        except: pass
        return {}
    def _load_creds(self):
        try:
            if CREDS_FILE.exists(): return json.loads(CREDS_FILE.read_text())
        except: pass
        return {}
    def _save_creds(self):
        try:
            if self.save_creds_var.get():
                CREDS_FILE.write_text(json.dumps({
                    "client_id":self.client_id_var.get().strip(),
                    "pin":self.pin_var.get().strip(),
                    "totp":self.totp_var.get().strip(),
                    "save":True},indent=2))
            elif CREDS_FILE.exists():
                CREDS_FILE.unlink()   # user unticked save → remove stored creds
        except: pass
    def _save(self):
        try:
            SETTINGS_FILE.write_text(json.dumps({
                "tf":self.tf_var.get(),"period":self.period_var.get(),
                "mult":self.mult_var.get(),"paper":self.paper_var.get(),
                "nifty_lots":self.nifty_lots_var.get(),"sensex_lots":self.sensex_lots_var.get(),
                "trade_nifty":self.trade_nifty_var.get(),"trade_sensex":self.trade_sensex_var.get(),
                "mcx":{n:{"trade":tv.get(),"lots":lv.get()} for n,(tv,lv) in self.mcx_vars.items()},
                "sq":self.sq_var.get()},indent=2))
        except: pass
    def _on_close(self): self._save(); self._save_creds(); self.destroy()

    # ── Build ──
    def _build(self):
        h=ctk.CTkFrame(self,fg_color=HBG,height=50,corner_radius=0); h.pack(fill="x"); h.pack_propagate(False)
        ctk.CTkLabel(h,text="BALFUND",font=("Segoe UI",20,"bold"),text_color=GOLD).pack(side="left",padx=14)
        ctk.CTkLabel(h,text="Supertrend  NIFTY + SENSEX  Options",font=("Segoe UI",12,"bold"),
            text_color="#93C5FD").pack(side="left")
        self._lbl_pnl=ctk.CTkLabel(h,text="Net P&L: ₹0.00",font=("Segoe UI",13,"bold"),text_color=GOLD)
        self._lbl_pnl.pack(side="right",padx=14)
        self._lbl_ws=ctk.CTkLabel(h,text="WS: --",font=("Segoe UI",10,"bold"),text_color="#93C5FD")
        self._lbl_ws.pack(side="right",padx=8)
        self._lbl_mode=ctk.CTkLabel(h,text="PAPER",font=("Segoe UI",11,"bold"),text_color="#FDE68A")
        self._lbl_mode.pack(side="right",padx=8)
        self._lbl_clk=ctk.CTkLabel(h,text="",font=("Segoe UI",10,"bold"),text_color="#93C5FD")
        self._lbl_clk.pack(side="right",padx=8)

        nb=ctk.CTkTabview(self,fg_color=BG,segmented_button_selected_color=AC,
            segmented_button_selected_hover_color=ACL,segmented_button_unselected_color=CARD,
            segmented_button_unselected_hover_color="#E5E7EB",
            text_color="#000000")
        nb.pack(fill="both",expand=True,padx=6,pady=(0,4))
        # Make tab button text black + bold and larger for visibility
        try:
            nb._segmented_button.configure(font=("Segoe UI",13,"bold"),
                text_color="#000000")
        except Exception:
            pass
        for t in ["Connection","Settings","Live","Log"]: nb.add(t)
        self._nb=nb
        self._build_connection(nb.tab("Connection"))
        self._build_settings(nb.tab("Settings"))
        self._build_live(nb.tab("Live"))
        self._build_log(nb.tab("Log"))

    def _build_connection(self, p):
        p.configure(fg_color=BG)
        wrap=ctk.CTkFrame(p,fg_color="transparent"); wrap.pack(expand=True)
        card=ctk.CTkFrame(wrap,fg_color=CARD,corner_radius=12,border_width=1,border_color=BD)
        card.pack(padx=20,pady=20)
        ctk.CTkLabel(card,text="Dhan Login",font=("Segoe UI",18,"bold"),
            text_color=AC).pack(pady=(18,2),padx=40)
        ctk.CTkLabel(card,text="Generate your access token directly here",
            font=("Segoe UI",11,"bold"),text_color=TD).pack(pady=(0,14))
        def field(label, var, show=None):
            ctk.CTkLabel(card,text=label,font=FONT,text_color=TD,anchor="w").pack(
                anchor="w",padx=40,pady=(6,2))
            e=ctk.CTkEntry(card,textvariable=var,width=340,height=38,font=FONT,
                show=show,fg_color=BG,text_color=TX,border_color=BD)
            e.pack(padx=40); return e
        field("Client ID", self.client_id_var)
        field("6-Digit PIN", self.pin_var, show="*")
        field("TOTP Secret Key", self.totp_var, show="*")
        ctk.CTkCheckBox(card,text="Save credentials locally (this PC only)",
            variable=self.save_creds_var,font=FONT,text_color=TD,
            checkbox_width=20,checkbox_height=20).pack(anchor="w",padx=40,pady=(12,6))
        self._btn_gen=ctk.CTkButton(card,text="🔑  Generate Token",font=("Segoe UI",13,"bold"),
            fg_color=AC,hover_color=ACL,height=42,width=340,command=self._gen_token)
        self._btn_gen.pack(padx=40,pady=(8,4))
        self._lbl_conn=ctk.CTkLabel(card,text="● Not connected",font=("Segoe UI",12,"bold"),
            text_color=RD)
        self._lbl_conn.pack(pady=(8,18))

    def _gen_token(self):
        cid=self.client_id_var.get().strip()
        pin=self.pin_var.get().strip()
        totp=self.totp_var.get().strip().replace(" ","")
        if not (cid and pin and totp):
            self._lbl_conn.configure(text="● Fill all 3 fields",text_color=RD); return
        self._lbl_conn.configure(text="● Generating...",text_color=GOLD)
        self._btn_gen.configure(state="disabled",text="Generating...")
        def work():
            try:
                res=api_generate_token(cid,pin,totp)
                if res.get("success"):
                    self._access_token=res["access_token"]
                    self._token_client=cid; self._token_ok=True
                    name=res.get("client_name","") or cid
                    self.after(0,lambda:self._lbl_conn.configure(
                        text=f"● Connected: {name}",text_color=GR))
                    self._log(f"[Token] Generated OK for {name}")
                    self._save_creds()
                else:
                    self._token_ok=False
                    err=res.get("error","Unknown error")
                    self.after(0,lambda:self._lbl_conn.configure(
                        text=f"● Failed: {err[:40]}",text_color=RD))
                    self._log(f"[Token] FAILED: {err}")
            except Exception as e:
                self._token_ok=False
                self.after(0,lambda:self._lbl_conn.configure(
                    text=f"● Error: {str(e)[:40]}",text_color=RD))
                self._log(f"[Token] ERROR: {e}")
            finally:
                self.after(0,lambda:self._btn_gen.configure(
                    state="normal",text="🔑  Generate Token"))
        threading.Thread(target=work,daemon=True).start()

    def _build_settings(self, p):
        p.configure(fg_color=BG); p.grid_columnconfigure((0,1,2),weight=1)
        def card(col,title):
            f=ctk.CTkFrame(p,fg_color=CARD,corner_radius=8,border_width=1,border_color=BD)
            f.grid(row=0,column=col,padx=8,pady=10,sticky="nsew")
            ctk.CTkLabel(f,text=title,font=("Segoe UI",12,"bold"),text_color=AC).pack(pady=(10,6),anchor="w",padx=12)
            return f
        # Supertrend + timeframe
        st=card(0,"Supertrend & Timeframe")
        ctk.CTkLabel(st,text="Timeframe",font=FONT,text_color=TD).pack(anchor="w",padx=18)
        for v,l in [("5s","5 Seconds"),("1","1 Minute"),("5","5 Minutes"),("15","15 Minutes")]:
            ctk.CTkRadioButton(st,text=l,variable=self.tf_var,value=v,font=FONT).pack(anchor="w",padx=20,pady=2)
        ctk.CTkFrame(st,fg_color=BD,height=1).pack(fill="x",padx=12,pady=6)
        for lbl,var,w in [("ST Period:",self.period_var,60),("ST Multiplier:",self.mult_var,60)]:
            r=ctk.CTkFrame(st,fg_color="transparent"); r.pack(fill="x",padx=14,pady=3)
            ctk.CTkLabel(r,text=lbl,width=110,anchor="w",font=FONT,text_color=TD).pack(side="left")
            ctk.CTkEntry(r,textvariable=var,width=w,justify="center",font=FONT).pack(side="left",padx=4)
        # Indices + lots
        ix=card(1,"Indices & Lots")
        ctk.CTkSwitch(ix,text="Trade NIFTY",variable=self.trade_nifty_var,font=FONT,
            button_color=AC,progress_color=ACL).pack(anchor="w",padx=12,pady=4)
        r1=ctk.CTkFrame(ix,fg_color="transparent"); r1.pack(fill="x",padx=14,pady=2)
        ctk.CTkLabel(r1,text="NIFTY lots:",width=110,anchor="w",font=FONT,text_color=TD).pack(side="left")
        ctk.CTkEntry(r1,textvariable=self.nifty_lots_var,width=60,justify="center",font=FONT).pack(side="left",padx=4)
        ctk.CTkFrame(ix,fg_color=BD,height=1).pack(fill="x",padx=12,pady=6)
        ctk.CTkSwitch(ix,text="Trade SENSEX",variable=self.trade_sensex_var,font=FONT,
            button_color=AC,progress_color=ACL).pack(anchor="w",padx=12,pady=4)
        r2=ctk.CTkFrame(ix,fg_color="transparent"); r2.pack(fill="x",padx=14,pady=2)
        ctk.CTkLabel(r2,text="SENSEX lots:",width=110,anchor="w",font=FONT,text_color=TD).pack(side="left")
        ctk.CTkEntry(r2,textvariable=self.sensex_lots_var,width=60,justify="center",font=FONT).pack(side="left",padx=4)
        ctk.CTkFrame(ix,fg_color=BD,height=1).pack(fill="x",padx=12,pady=6)
        ctk.CTkLabel(ix,text="MCX Futures (long/short)",font=FONT,text_color=AC).pack(anchor="w",padx=12,pady=(2,2))
        for _name,(tv,lv) in self.mcx_vars.items():
            row=ctk.CTkFrame(ix,fg_color="transparent"); row.pack(fill="x",padx=12,pady=1)
            ctk.CTkSwitch(row,text=_name,variable=tv,font=FONT,width=150,
                button_color=AC,progress_color=ACL).pack(side="left")
            ctk.CTkEntry(row,textvariable=lv,width=50,justify="center",font=FONT).pack(side="left",padx=4)
            ctk.CTkLabel(row,text="lots",font=("Segoe UI",10,"bold"),text_color=GY).pack(side="left")
        # Control
        cf=card(2,"Control")
        ctk.CTkSwitch(cf,text="Paper Mode (safe)",variable=self.paper_var,font=FONT,
            button_color=AC,progress_color=ACL).pack(anchor="w",padx=12,pady=6)
        r3=ctk.CTkFrame(cf,fg_color="transparent"); r3.pack(fill="x",padx=14,pady=3)
        ctk.CTkLabel(r3,text="Square-off:",width=90,anchor="w",font=FONT,text_color=TD).pack(side="left")
        ctk.CTkEntry(r3,textvariable=self.sq_var,width=70,justify="center",font=FONT).pack(side="left",padx=4)
        ctk.CTkFrame(cf,fg_color=BD,height=1).pack(fill="x",padx=12,pady=6)
        self._lbl_status=ctk.CTkLabel(cf,text="Ready",font=FONT,text_color=TD,wraplength=180)
        self._lbl_status.pack(padx=12,pady=4)
        self._btn_start=ctk.CTkButton(cf,text="▶  START",font=("Segoe UI",13,"bold"),
            fg_color=GR,hover_color="#047857",command=self._start,height=38)
        self._btn_start.pack(fill="x",padx=12,pady=4)
        self._btn_stop=ctk.CTkButton(cf,text="■  STOP",font=("Segoe UI",13,"bold"),
            fg_color=RD,hover_color="#991B1B",command=self._stop,state="disabled",height=38)
        self._btn_stop.pack(fill="x",padx=12,pady=4)
        ctk.CTkButton(cf,text="⬛  Square Off All",font=FONT,fg_color="#374151",
            hover_color="#111827",height=30,
            command=lambda:threading.Thread(target=self.engine.sq_all,daemon=True).start() if self.engine else None
            ).pack(fill="x",padx=12,pady=4)

    def _build_live(self, p):
        p.configure(fg_color=BG)
        tb=ctk.CTkFrame(p,fg_color=CARD,corner_radius=6,border_width=1,border_color=BD,height=34)
        tb.pack(fill="x",padx=6,pady=(6,2)); tb.pack_propagate(False)
        self._lbl_next=ctk.CTkLabel(tb,text="Next candle: --",font=FONT,text_color=TD)
        self._lbl_next.pack(side="right",padx=10)
        tf=tk.Frame(p,bg=BG); tf.pack(fill="both",expand=True,padx=6,pady=4)
        style=ttk.Style(); style.theme_use("clam")
        style.configure("ST.Treeview",background=BG,foreground=TX,fieldbackground=BG,
            font=("Segoe UI",11,"bold"),rowheight=34,borderwidth=0)
        style.configure("ST.Treeview.Heading",background=HBG,foreground="white",
            font=("Segoe UI",11,"bold"))
        style.map("ST.Treeview",background=[("selected","#DBEAFE")],foreground=[("selected",TX)])
        self._tree=ttk.Treeview(tf,columns=TREE_COLS,show="headings",style="ST.Treeview",height=6)
        for c,w in zip(TREE_COLS,COL_W):
            self._tree.heading(c,text=c); self._tree.column(c,width=w,anchor="center")
        self._tree.pack(fill="both",expand=True)
        self._tree.tag_configure("green",foreground=GR)
        self._tree.tag_configure("red",foreground=RD)
        self._tree_ids={}

    def _build_log(self, p):
        p.configure(fg_color=BG)
        tb=ctk.CTkFrame(p,fg_color=CARD,corner_radius=6,height=30); tb.pack(fill="x",padx=6,pady=(6,2)); tb.pack_propagate(False)
        ctk.CTkButton(tb,text="Clear",width=70,height=22,font=FONT,fg_color=BD,text_color=TX,
            hover_color="#E5E7EB",command=lambda:self._log_box.delete("1.0","end")).pack(side="right",padx=6,pady=4)
        self._log_box=ctk.CTkTextbox(p,font=("Consolas",11,"bold"),fg_color=CARD,text_color=TX,
            border_width=1,border_color=BD)
        self._log_box.pack(fill="both",expand=True,padx=6,pady=(2,6))

    def _log(self, msg):
        self._log_q.put(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}\n")
    def _flush(self):
        lines=[]
        try:
            while True: lines.append(self._log_q.get_nowait())
        except queue.Empty: pass
        if lines:
            self._log_box.insert("end","".join(lines)); self._log_box.see("end")
    def _set_status(self,m,c=TD): self._lbl_status.configure(text=m,text_color=c)

    def _start(self):
        if self._running: return
        if not self._token_ok or not self._access_token:
            self._set_status("Generate token first (Connection tab)",RD)
            self._nb.set("Connection"); return
        cid=self._token_client; tok=self._access_token
        mcx_sel={n:lv.get() for n,(tv,lv) in self.mcx_vars.items() if tv.get()}
        any_index=self.trade_nifty_var.get() or self.trade_sensex_var.get()
        if not (any_index or mcx_sel):
            self._set_status("Enable at least one instrument",RD); return
        tf=self.tf_var.get(); iv=5 if tf=="5s" else int(tf)*60
        self.engine=Engine(cid,tok,iv,max(1,self.period_var.get()),
            max(0.1,self.mult_var.get()),self.paper_var.get(),
            self.nifty_lots_var.get(),self.sensex_lots_var.get(),
            self.sq_var.get(),self.trade_nifty_var.get(),self.trade_sensex_var.get(),
            mcx_sel,
            log_fn=self._log)
        # Build tree rows
        for iid in self._tree.get_children(): self._tree.delete(iid)
        self._tree_ids={}
        for t in self.engine.traders:
            iid=self._tree.insert("","end",values=(t.idx["name"],"-","-","-","-","FLAT","-","-","-","-","-","Ready"))
            self._tree_ids[t.idx["name"]]=iid
        for name in mcx_sel:
            iid=self._tree.insert("","end",values=(name,"-","-","-","-","FLAT","FUT","-","-","-","-","Resolving..."))
            self._tree_ids[name]=iid
        self.engine.start(); self._running=True
        self._btn_start.configure(state="disabled",text="RUNNING ●")
        self._btn_stop.configure(state="normal")
        mode="PAPER" if self.paper_var.get() else "LIVE"
        self._lbl_mode.configure(text=mode,text_color="#FDE68A" if mode=="PAPER" else RD)
        self._set_status(f"Running | {tf} | P{self.period_var.get()} M{self.mult_var.get()} | {mode}",GR)
        self._save(); self._nb.set("Live")

    def _stop(self):
        if self.engine: self.engine.stop()
        self._running=False
        self._btn_start.configure(state="normal",text="▶  START")
        self._btn_stop.configure(state="disabled")
        self._set_status("Stopped",TD)

    def _tick(self):
        self._lbl_clk.configure(text=datetime.now().strftime("%d %b %Y  %H:%M:%S"))
        self._flush()
        if self.engine:
            ws=self.engine.ws_status
            wsc=GR if "onnected" in ws else (RD if "rror" in ws or "isconnect" in ws else GY)
            self._lbl_ws.configure(text=f"WS: {ws[:22]}  t:{self.engine.feed.ticks}",text_color=wsc)
            self._lbl_next.configure(text=f"Next candle: {self.engine.next_candle}")
            total=0.0
            for t in self.engine.traders:
                iid=self._tree_ids.get(t.idx["name"])
                if not iid: continue
                pnl=t.pnl; total+=pnl
                tag=("green",) if t.st_dir=="GREEN" else (("red",) if t.st_dir=="RED" else ())
                self._tree.item(iid,values=(
                    t.idx["name"],
                    f"{t.spot:.2f}" if t.spot else "-",
                    f"{t.st_value:.2f}" if t.st_value else "-",
                    t.st_dir,
                    f"{t.atr:.2f}" if t.atr else "-",
                    t.position,
                    str(t.strike) if t.strike else "-",
                    f"{t.entry:.2f}" if t.entry else "-",
                    f"{t.opt_ltp:.2f}" if t.opt_ltp else "-",
                    f"₹{pnl:+.2f}" if t.position!="FLAT" else "-",
                    t.bar_time, t.status), tags=tag)
            # Render future traders (Silver)
            for f in self.engine.futures:
                iid=self._tree_ids.get(f.name)
                if not iid: continue
                pnl=f.pnl; total+=pnl
                tag=("green",) if f.st_dir=="GREEN" else (("red",) if f.st_dir=="RED" else ())
                self._tree.item(iid,values=(
                    f.name,
                    f"{f.ltp:.2f}" if f.ltp else "-",
                    f"{f.st_value:.2f}" if f.st_value else "-",
                    f.st_dir,
                    f"{f.atr:.2f}" if f.atr else "-",
                    f.position,
                    "FUT",
                    f"{f.entry:.2f}" if f.entry else "-",
                    f"{f.ltp:.2f}" if f.ltp else "-",
                    f"₹{pnl:+.2f}" if f.position!="FLAT" else "-",
                    f.bar_time, f.status), tags=tag)
            c=GR if total>0 else (RD if total<0 else GOLD)
            self._lbl_pnl.configure(text=f"Net P&L: ₹{total:+,.2f}",text_color=c)
        self.after(500,self._tick)

def main():
    try: App().mainloop()
    except Exception:
        import traceback, tkinter.messagebox as mb
        ctk.CTk().withdraw()
        mb.showerror("Balfund Supertrend — Error", f"Crash:\n\n{traceback.format_exc()}")

if __name__=="__main__":
    main()
