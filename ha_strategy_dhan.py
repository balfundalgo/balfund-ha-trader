"""
Balfund HA + SMA Strategy Trader  v3.0
Architecture: ttk.Treeview for instrument list (handles 200 rows instantly),
              batch log queue, WS-only candles, single preload thread.
"""
from __future__ import annotations
import os, sys, time, json, struct, threading, csv, io, queue
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Tuple
from pathlib import Path
import requests, websocket
import customtkinter as ctk
import tkinter as tk
from tkinter import ttk

# ─────────────────────────────────────────────────────────────────────────────
#  PATHS & CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR          = Path(sys.executable).parent if getattr(sys,"frozen",False) else Path(__file__).parent
SHARED_TOKEN_FILE = Path(r"C:\balfund_shared\dhan_token.json")
MASTER_CACHE      = BASE_DIR / "dhan_master_cache.csv"
MASTER_URL        = "https://images.dhan.co/api-data/api-scrip-master.csv"
INTRADAY_URL      = "https://api.dhan.co/v2/charts/intraday"
OC_EXPIRY_URL     = "https://api.dhan.co/v2/optionchain/expirylist"
OC_URL            = "https://api.dhan.co/v2/optionchain"
ORDER_URL         = "https://api.dhan.co/v2/orders"
WS_URL            = "wss://api-feed.dhan.co?version=2&token={tok}&clientId={cid}&authType=2"
NIFTY_SID         = "13"
NIFTY_SEG         = "IDX_I"
REQ_SUB           = 15
RESP_TICK         = 2
HDR_FMT           = ">H H I I H f"
SEG_MAP           = {1:"NSE_EQ",2:"NSE_FNO",3:"BSE_EQ",8:"MCX_COMM",9:"IDX_I"}
MCX_LOT_MULT      = {"GOLDTEN":1,"SILVERMICRO":1,"CRUDEOILM":10,"ZINCMINI":1000,"GOLDPETAL":1}
NSE_STOCKS = [
    "GMRAIRPORT","IRFC","DBREALTY","DEVYANI","VMM","IREDA","WELSPUNLIV","PNB",
    "JMFINANCIL","J&KBANK","IEX","JSWCEMENT","MOTHERSON","RCF","NIACL","ADANIPOWER",
    "IRCON","CANBK","SAMMAANCAP","NCC","GAIL","SAIL","PPLPHARMA","CESC","BANKINDIA",
    "IGL","IOC","ITCHOTELS","CGCL","JINDALSAW","SAPPHIRE","AWL","HUDCO","BANDHANBNK",
    "CASTROLIND","FINPIPE","UNIONBANK","ASHOKLEY","AEGISVOPAK","MRPL","TATASTEEL",
    "ENGINERSIN","WIPRO","RITES","FSL","FIRSTCRY","ACMESOLAR","ANGELONE","ETERNAL",
    "APTUS","JIOFIN","CAMPUS","JYOTHYLAB","SCI","NLCINDIA","CROMPTON","SONATSOFTW",
    "BLS","GPIL","CUB","ITI","BHEL","NYKAA","REDINGTON","MANAPPURAM","JSWINFRA",
    "ONGC","LTF","PCBL","AFCONS","FEDERALBNK","GSPL","RVNL","JWL","RAILTEL","TARIL",
    "NUVOCO","PETRONET","HONASA","COHANCE","BANKBARODA","SWIGGY","POWERGRID",
    "LATENTVIEW","KARURVYSYA","RBLBANK","ITC","PRAJIND","EXIDEIND","EIHOTEL","VGUARD",
    "BPCL","SAREGAMA","IGIL","ABCAPITAL","GODIGIT","RECLTD","TMPV","SWANCORP","M&MFIN",
    "MANYAVAR","GICRE","INDIACEM","TRIVENI","GUJGASLTD","BLUEJET","NTPC","LTFOODS",
    "TATAPOWER","RHIM","BSOFT","HINDPETRO","NATIONALUM","FIVESTAR","KALYANKJIL",
    "SUMICHEM","KOTAKBANK","BIOCON","HAPPSTMNDS","ELECON","SYNGENE","PFC","USHAMART",
    "POONAWALLA","DELHIVERY","AARTIIND","THELEELA","CHAMBLFERT","APOLLOTYRE","VBL",
    "BERGEPAINT","HEXT","COALINDIA","EMAMILTD","HSCL","JKTYRE","INDUSTOWER",
    "AGARWALEYE","TEJASNET","STARHEALTH","INDGN","BEL","AMBUJACEM","NEWGEN",
    "TRITURBINE","CONCOR","ATGL","OIL","DABUR","AADHARHFC","ANANTRAJ","JUBLFOOD",
    "AIIL","JSWENERGY","IIFL","PATANJALI","AKUMS","BALRAMCHIN","MINDACORP","SONACOMS",
    "LICHSGFIN","JBMA","HEG","ELGIEQUIP","KEC","VTL","GMDCLTD","MAHSEAMLES","IRCTC",
    "SARDAEN","RKFORGE","ZENSARTECH","JUBLINGREA",
]
MCX_SYMS = ["GOLDTEN","SILVERMICRO","CRUDEOILM","ZINCMINI","GOLDPETAL"]

# ─────────────────────────────────────────────────────────────────────────────
#  THEME
# ─────────────────────────────────────────────────────────────────────────────
BG   = "#FFFFFF"; CARD = "#F0F4FA"; HBG = "#1E3A5F"
AC   = "#1A56DB"; ACL  = "#3B82F6"; GOLD = "#D4A017"
GR   = "#059669"; RD   = "#DC2626"; GY  = "#9CA3AF"
TX   = "#111827"; TD   = "#6B7280"; BD  = "#D1D5DB"
FONT = ("Segoe UI", 11, "bold")

# ─────────────────────────────────────────────────────────────────────────────
#  DATA
# ─────────────────────────────────────────────────────────────────────────────
class InstrState:
    __slots__ = ["name","sid","seg","is_mcx","lot_mult","qty",
                 "position","entry","ltp","ha_open","ha_close",
                 "color","signal","status","bar_time","last_tick",
                 "skip","sq_done","agg"]
    def __init__(self, name, sid, seg, is_mcx=False, lot_mult=1, qty=1):
        self.name=name; self.sid=sid; self.seg=seg
        self.is_mcx=is_mcx; self.lot_mult=lot_mult; self.qty=qty
        self.position="FLAT"; self.entry=0.0; self.ltp=0.0
        self.ha_open=0.0; self.ha_close=0.0; self.color="-"
        self.signal=""; self.status="Ready"; self.bar_time="-"
        self.last_tick=0.0; self.skip=False; self.sq_done=False; self.agg=None
    @property
    def pnl(self):
        if self.position=="FLAT" or self.entry==0: return 0.0
        d=(self.ltp-self.entry) if self.position=="LONG" else (self.entry-self.ltp)
        return round(d*self.qty*self.lot_mult,2)

# ─────────────────────────────────────────────────────────────────────────────
#  CANDLE AGGREGATOR
# ─────────────────────────────────────────────────────────────────────────────
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
                if p>c["high"]: c["high"]=p
                if p<c["low"]:  c["low"]=p
                c["close"]=p
    def candles(self):
        with self._l: return list(self._c)
    def ready(self):
        with self._l: return len(self._c)>=2

# ─────────────────────────────────────────────────────────────────────────────
#  HA + SMA
# ─────────────────────────────────────────────────────────────────────────────
def compute_ha(candles, sma=1):
    ha=[]
    for i,c in enumerate(candles):
        o,h,l,cl=float(c["open"]),float(c["high"]),float(c["low"]),float(c["close"])
        hac=(o+h+l+cl)/4; hao=(ha[-1]["open"]+ha[-1]["close"])/2 if i else (o+cl)/2
        ha.append({"bucket":c["bucket"],"open":hao,
                   "high":max(h,hao,hac),"low":min(l,hao,hac),"close":hac})
    if sma<=1: return ha
    sha=[]
    for i,b in enumerate(ha):
        if i<sma-1: sha.append(dict(b))
        else:
            sc=sum(ha[j]["close"] for j in range(i-sma+1,i+1))/sma
            so=sum(ha[j]["open"]  for j in range(i-sma+1,i+1))/sma
            sha.append({"bucket":b["bucket"],"open":so,
                        "high":max(b["high"],so,sc),"low":min(b["low"],so,sc),"close":sc})
    return sha

def ha_color(c): return "GREEN" if c["close"]>c["open"] else ("RED" if c["close"]<c["open"] else "DOJI")

# ─────────────────────────────────────────────────────────────────────────────
#  TOKEN
# ─────────────────────────────────────────────────────────────────────────────
def load_token():
    try:
        if SHARED_TOKEN_FILE.exists():
            d=json.loads(SHARED_TOKEN_FILE.read_text())
            return d.get("client_id",""), d.get("access_token","")
    except: pass
    try:
        r=requests.get("http://localhost:5555/token",timeout=3)
        d=r.json(); return d.get("client_id",""), d.get("access_token","")
    except: pass
    return os.getenv("DHAN_CLIENT_ID",""), os.getenv("DHAN_ACCESS_TOKEN","")

# ─────────────────────────────────────────────────────────────────────────────
#  REST — OHLC
# ─────────────────────────────────────────────────────────────────────────────
def fetch_ohlc(cid, tok, sid, seg, iv, days=5):
    now=datetime.now()
    hdrs={"Content-Type":"application/json","access-token":tok,"client-id":cid}
    instr="INDEX" if seg=="IDX_I" else "EQUITY"
    pl={"securityId":sid,"exchangeSegment":seg,"instrument":instr,"expiryCode":0,
        "oi":False,"interval":iv,
        "fromDate":(now-timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),
        "toDate":now.strftime("%Y-%m-%d %H:%M:%S")}
    for att in range(3):
        r=requests.post(INTRADAY_URL,headers=hdrs,json=pl,timeout=20)
        if r.status_code==429: time.sleep(2*(att+1)); continue
        r.raise_for_status(); break
    d=r.json()
    opens=d.get("open",[]); highs=d.get("high",[]); lows=d.get("low",[]); closes=d.get("close",[])
    times=d.get("timestamp",d.get("start_Time",[]))
    out=[]
    for i in range(len(opens)):
        ts=times[i]
        if isinstance(ts,str):
            try: epoch=int(datetime.strptime(ts,"%Y-%m-%d %H:%M:%S").timestamp())
            except: epoch=0
        else: epoch=int(ts)
        out.append({"bucket":epoch,"open":float(opens[i]),"high":float(highs[i]),
                    "low":float(lows[i]),"close":float(closes[i])})
    return out

# ─────────────────────────────────────────────────────────────────────────────
#  MASTER CSV + RESOLVER
# ─────────────────────────────────────────────────────────────────────────────
import re as _re

def load_master(log_fn=print):
    if MASTER_CACHE.exists():
        age=(time.time()-MASTER_CACHE.stat().st_mtime)/3600
        if age<12:
            log_fn(f"  Using cached CSV ({age:.1f}h old)")
            raw=MASTER_CACHE.read_text(encoding="utf-8",errors="ignore")
            if raw.startswith("\ufeff"): raw=raw[1:]
            return list(csv.DictReader(io.StringIO(raw)))
    log_fn("  Downloading master CSV...")
    r=requests.get(MASTER_URL,stream=True,timeout=120); r.raise_for_status()
    total=int(r.headers.get("content-length",0))
    buf=io.BytesIO(); done=0; last_pct=-1
    for chunk in r.iter_content(65536):
        buf.write(chunk); done+=len(chunk)
        if total:
            pct=(done*100//total//10)*10
            if pct>last_pct: log_fn(f"    {done/1e6:.1f}/{total/1e6:.1f}MB ({pct}%)"); last_pct=pct
    raw=buf.getvalue().decode("utf-8",errors="ignore")
    if raw.startswith("\ufeff"): raw=raw[1:]
    MASTER_CACHE.write_text(raw,encoding="utf-8")
    return list(csv.DictReader(io.StringIO(raw)))

def build_nse_index(rows):
    idx={}
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID","").strip().upper()!="NSE": continue
        series=row.get("SEM_SERIES","").strip().upper()
        if series not in ("EQ","BE","BZ","IL","SM"): continue
        ts=row.get("SEM_TRADING_SYMBOL","").strip().upper()
        sid=row.get("SEM_SMST_SECURITY_ID","").strip()
        if not ts or not sid: continue
        ticker=_re.sub(r"-(EQ|BE|BZ|IL|SM)$","",ts).strip()
        if ticker:
            if series=="EQ": idx[ticker]=sid
            elif ticker not in idx: idx[ticker]=sid
    return idx

def resolve_mcx(rows, sym, log_fn=print):
    today=date.today(); safe=today+timedelta(days=2)
    MONTHS={"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,
            "JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}
    variants=[sym.upper(), sym.upper().replace("MICRO","MIC"), sym.upper().replace("MINI","")]
    found=[]
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID","").strip().upper()!="MCX": continue
        ts=row.get("SEM_TRADING_SYMBOL","").strip().upper()
        if not any(ts.startswith(v) for v in variants) or not ts.endswith("-FUT"): continue
        m=_re.search(r"(\d{2})([A-Z]{3})(\d{4})",ts)
        if not m: continue
        try: exp=date(int(m.group(3)),MONTHS[m.group(2)],int(m.group(1)))
        except: continue
        sid=row.get("SEM_SMST_SECURITY_ID","").strip()
        found.append((exp,sid,ts))
    active=sorted([x for x in found if x[0]>=safe],key=lambda x:x[0])
    if not active: active=sorted([x for x in found if x[0]>=today],key=lambda x:x[0])
    if not active: return None
    e,sid,ts=active[0]
    log_fn(f"  ✓ {sym:<12} sid={sid}  {ts}  expiry={e}")
    return {"sid":sid,"ts":ts,"expiry":str(e)}

# ─────────────────────────────────────────────────────────────────────────────
#  ORDER
# ─────────────────────────────────────────────────────────────────────────────
def place_order(cid,tok,seg,sid,side,qty,paper,log_fn=print):
    if paper:
        log_fn(f"[PAPER] {side} qty={qty} {seg}:{sid}")
        return 0.0
    hdrs={"Content-Type":"application/json","access-token":tok,"client-id":cid}
    pl={"dhanClientId":cid,"transactionType":side,"exchangeSegment":seg,
        "productType":"INTRADAY","orderType":"MARKET","validity":"DAY",
        "securityId":sid,"quantity":qty}
    r=requests.post(ORDER_URL,headers=hdrs,json=pl,timeout=15); r.raise_for_status()
    return float(r.json().get("avgPrice",0) or 0)

# ─────────────────────────────────────────────────────────────────────────────
#  WEBSOCKET FEED
# ─────────────────────────────────────────────────────────────────────────────
class Feed:
    def __init__(self, cid, tok, insts, on_tick, on_status):
        self.cid=cid; self.tok=tok; self.insts=insts
        self._on_tick=on_tick; self._on_status=on_status
        self._ws=None; self._stop=threading.Event()
        self.connected=False; self.ticks=0
    def start(self): threading.Thread(target=self._run,daemon=True).start()
    def stop(self): self._stop.set(); self._ws and self._close_ws()
    def _close_ws(self):
        try: self._ws.close()
        except: pass
    def subscribe(self,sid,seg):
        if self._ws:
            try: self._ws.send(json.dumps({"RequestCode":REQ_SUB,"InstrumentCount":1,
                    "InstrumentList":[{"ExchangeSegment":seg,"SecurityId":sid}]}))
            except: pass
    def _run(self):
        delay=5
        while not self._stop.is_set():
            url=WS_URL.format(tok=self.tok,cid=self.cid)
            self._ws=websocket.WebSocketApp(url,on_open=self._open,
                on_message=self._msg,
                on_error=lambda ws,e: self._on_status(f"WS error: {e}"),
                on_close=lambda ws,*a: self._on_status("WS disconnected"))
            self._ws.run_forever(ping_interval=20,ping_timeout=10)
            self.connected=False
            if not self._stop.is_set():
                self._on_status(f"WS reconnecting in {delay}s...")
                time.sleep(delay); delay=min(delay*2,60)
    def _open(self,ws):
        BATCH=100; total=len(self.insts)
        for i in range(0,total,BATCH):
            batch=self.insts[i:i+BATCH]
            ws.send(json.dumps({"RequestCode":REQ_SUB,"InstrumentCount":len(batch),
                "InstrumentList":[{"ExchangeSegment":seg,"SecurityId":sid} for sid,seg in batch]}))
            if i+BATCH<total: time.sleep(0.1)
        self.connected=True; self._on_status(f"WS connected — {total} instruments")
    def _msg(self,ws,raw):
        try:
            b=bytes(raw)
            if len(b)<18: return
            hdr=struct.unpack_from(HDR_FMT,b,0)
            if int(hdr[1])!=RESP_TICK: return
            sid=str(int(hdr[2])); seg_c=int(hdr[3]); ltp=float(hdr[5])
            if ltp<=0: return
            self.ticks+=1; seg=SEG_MAP.get(seg_c,"")
            self._on_tick(sid,seg,ltp)
        except: pass

# ─────────────────────────────────────────────────────────────────────────────
#  ENGINE
# ─────────────────────────────────────────────────────────────────────────────
class Engine:
    def __init__(self,cid,tok,insts,iv_sec,sma,paper,nse_sq,mcx_sq,log_fn=print):
        self.cid=cid;self.tok=tok;self.insts=insts
        self.iv_sec=iv_sec;self.sma=sma;self.paper=paper
        self.nse_sq=nse_sq;self.mcx_sq=mcx_sq;self.log=log_fn
        self._lock=threading.Lock();self._stop=threading.Event()
        self._aggs:Dict[str,CandleAgg]={}
        self._sid_map:Dict[str,InstrState]={}
        for st in insts:
            k=f"{st.seg}:{st.sid}"
            self._sid_map[k]=st; self._sid_map[st.sid]=st
            self._aggs[k]=CandleAgg(iv_sec)
        self._aggs[f"{NIFTY_SEG}:{NIFTY_SID}"]=CandleAgg(iv_sec)
        feed_insts=[(st.sid,st.seg) for st in insts]+[(NIFTY_SID,NIFTY_SEG)]
        self._feed=Feed(cid,tok,feed_insts,self._tick,self._ws_status)
        self.ws_status="Not started"; self.next_candle="-"; self._startup_done=set()
    def start(self): threading.Thread(target=self._run,daemon=True).start()
    def stop(self): self._stop.set(); self._feed.stop()
    def sq_all(self):
        for st in self.insts:
            if st.position!="FLAT" and not st.sq_done:
                self._close(st,"SqOff"); st.sq_done=True; time.sleep(0.12)
    def _tick(self,sid,seg,ltp):
        key=f"{seg}:{sid}" if seg else sid
        st=self._sid_map.get(key) or self._sid_map.get(sid)
        now=time.time()
        if st:
            with self._lock: st.ltp=round(ltp,2); st.last_tick=now
            agg=self._aggs.get(key) or self._aggs.get(f"{st.seg}:{st.sid}")
            if agg: agg.on_tick(ltp,now)
            return
        if sid==NIFTY_SID:
            agg=self._aggs.get(f"{NIFTY_SEG}:{NIFTY_SID}")
            if agg: agg.on_tick(ltp,now)
    def _ws_status(self,msg):
        self.ws_status=msg; self.log(f"[WS] {msg}")
    def _run(self):
        # Seed
        rest_iv="1" if self.iv_sec<=60 else "5" if self.iv_sec<=300 else "15"
        all_seed=list(self.insts)+[None]
        self.log(f"[Engine] Seeding {len(all_seed)} instruments...")
        for i,st in enumerate(all_seed):
            if self._stop.is_set(): break
            try:
                if st is None:
                    c=fetch_ohlc(self.cid,self.tok,NIFTY_SID,NIFTY_SEG,rest_iv)
                    self._aggs[f"{NIFTY_SEG}:{NIFTY_SID}"]=CandleAgg(self.iv_sec,c[-100:])
                else:
                    c=fetch_ohlc(self.cid,self.tok,st.sid,st.seg,rest_iv)
                    self._aggs[f"{st.seg}:{st.sid}"]=CandleAgg(self.iv_sec,c[-100:])
                    with self._lock:
                        if c: st.ltp=float(c[-1]["close"])
            except Exception as e:
                nm="NIFTY" if st is None else st.name
                self.log(f"[seed ERR] {nm}: {e}")
            if (i+1)%5==0: time.sleep(1.0)
        self.log("[Engine] Seeding done — starting WS")
        self._feed.start(); time.sleep(2)
        self._process(startup=True); self._set_next()
        while not self._stop.is_set():
            self._wait(); self._check_sq(); self._process(startup=False); self._set_next()
        self.log("[Engine] Stopped")
    def _set_next(self):
        n=(int(time.time())//self.iv_sec+1)*self.iv_sec
        self.next_candle=datetime.fromtimestamp(n).strftime("%H:%M:%S")
    def _wait(self):
        n=(int(time.time())//self.iv_sec+1)*self.iv_sec
        while time.time()<n and not self._stop.is_set(): time.sleep(0.05)
    def _check_sq(self):
        t=datetime.now().strftime("%H:%M")
        for st in self.insts:
            if st.sq_done: continue
            sq=self.mcx_sq if st.is_mcx else self.nse_sq
            if t>=sq and st.position!="FLAT":
                self.log(f"[SqOff] {st.name}")
                self._close(st,"SqOff"); st.sq_done=True; time.sleep(0.12)
    def _process(self,startup):
        GAP=0.12
        mcx=[s for s in self.insts if s.is_mcx and not s.skip and not s.sq_done]
        nse=[s for s in self.insts if not s.is_mcx and not s.skip and not s.sq_done]
        for st in mcx+nse:
            if self._stop.is_set(): break
            agg=self._aggs.get(f"{st.seg}:{st.sid}")
            if not agg or not agg.ready(): continue
            candles=agg.candles()
            if len(candles)<2: continue
            ha=compute_ha(candles,self.sma); last=ha[-1]
            col=ha_color(last)
            bar_ts=datetime.fromtimestamp(last["bucket"]).strftime("%H:%M")
            with self._lock:
                st.ha_open=round(last["open"],2); st.ha_close=round(last["close"],2)
                st.color=col; st.bar_time=bar_ts
                st.signal="" if col=="DOJI" else ("BUY" if col=="GREEN" else "SELL")
            if col=="DOJI": continue
            need="LONG" if col=="GREEN" else "SHORT"
            if startup and st.name not in self._startup_done:
                self._startup_done.add(st.name)
                if st.position!=need:
                    if st.position!="FLAT": self._close(st,"Rev")
                    self._open(st,need); time.sleep(GAP)
                continue
            if startup: continue
            if st.position!=need:
                if st.position!="FLAT": self._close(st,"Rev")
                self._open(st,need); time.sleep(GAP)
            else:
                with self._lock: st.status=f"↑LONG" if need=="LONG" else "↓SHORT"
    def _open(self,st,direction):
        side="BUY" if direction=="LONG" else "SELL"
        price=place_order(self.cid,self.tok,st.seg,st.sid,side,st.qty,self.paper,self.log)
        with self._lock:
            st.position=direction; st.entry=price or st.ltp
            st.status=f"↑LONG" if direction=="LONG" else "↓SHORT"
        self.log(f"[{'PAPER' if self.paper else 'LIVE'}] {st.name} {side} @{st.entry:.2f}")
    def _close(self,st,reason):
        pnl=st.pnl; side="SELL" if st.position=="LONG" else "BUY"
        self.log(f"[{reason}] {st.name} {side}  P&L=₹{pnl:+.2f}")
        place_order(self.cid,self.tok,st.seg,st.sid,side,st.qty,self.paper,self.log)
        with self._lock: st.position="FLAT"; st.entry=0.0; st.status="FLAT"

# ─────────────────────────────────────────────────────────────────────────────
#  GUI
# ─────────────────────────────────────────────────────────────────────────────
ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

TREE_COLS = ["Instrument","Seg","HA-O","HA-C","HA","Signal","Pos",
             "Entry","Qty","LTP","P&L","Bar","Status"]
COL_W     = [110,60,72,72,75,55,55,76,45,76,80,50,150]

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Balfund HA + SMA  Strategy Trader  v3.0")
        self.geometry("1460x880"); self.configure(fg_color=BG)
        # State
        self.engine:Optional[Engine]=None
        self.instruments:List[InstrState]=[]
        self._master_rows:List[dict]=[]
        self._running=False
        self._log_q:queue.Queue=queue.Queue()
        self._stock_qty_vars:Dict[str,ctk.IntVar]={}
        # Settings vars
        self.tf_var=ctk.StringVar(value="1")
        self.sma_var=ctk.IntVar(value=1)
        self.paper_var=ctk.BooleanVar(value=True)
        self.nse_sq_var=ctk.StringVar(value="15:15")
        self.mcx_sq_var=ctk.StringVar(value="23:25")
        self.gold_v=ctk.IntVar(value=1); self.silv_v=ctk.IntVar(value=1)
        self.crude_v=ctk.IntVar(value=1); self.zinc_v=ctk.IntVar(value=1)
        self.gp_v=ctk.IntVar(value=1)
        self.default_qty_v=ctk.IntVar(value=1)
        self._build()
        self.after(300, self._preload)
        self.after(400, self._tick)

    # ── Build ────────────────────────────────────────────────────────────────
    def _build(self):
        self._build_header()
        self._nb=ctk.CTkTabview(self,fg_color=BG,
            segmented_button_selected_color=AC,
            segmented_button_selected_hover_color=ACL,
            segmented_button_unselected_color=CARD)
        self._nb.pack(fill="both",expand=True,padx=6,pady=(0,4))
        for t in ["Settings","Live Strategy","Log"]: self._nb.add(t)
        self._build_settings(self._nb.tab("Settings"))
        self._build_live(self._nb.tab("Live Strategy"))
        self._build_log(self._nb.tab("Log"))

    def _build_header(self):
        h=ctk.CTkFrame(self,fg_color=HBG,height=50,corner_radius=0)
        h.pack(fill="x"); h.pack_propagate(False)
        ctk.CTkLabel(h,text="BALFUND",font=("Segoe UI",20,"bold"),
            text_color=GOLD).pack(side="left",padx=14)
        ctk.CTkLabel(h,text="HA + SMA  Strategy Trader  v3.0",
            font=("Segoe UI",12,"bold"),text_color="#93C5FD").pack(side="left")
        self._lbl_pnl=ctk.CTkLabel(h,text="Net P&L: ₹0.00",
            font=("Segoe UI",13,"bold"),text_color=GOLD)
        self._lbl_pnl.pack(side="right",padx=14)
        self._lbl_ws=ctk.CTkLabel(h,text="WS: --",
            font=("Segoe UI",10,"bold"),text_color="#93C5FD")
        self._lbl_ws.pack(side="right",padx=8)
        self._lbl_mode=ctk.CTkLabel(h,text="PAPER",
            font=("Segoe UI",11,"bold"),text_color="#FDE68A")
        self._lbl_mode.pack(side="right",padx=8)
        self._lbl_clk=ctk.CTkLabel(h,text="",
            font=("Segoe UI",10,"bold"),text_color="#93C5FD")
        self._lbl_clk.pack(side="right",padx=8)

    def _build_settings(self,parent):
        parent.configure(fg_color=BG)
        parent.grid_columnconfigure((0,1,2,3),weight=1)
        def card(col,title):
            f=ctk.CTkFrame(parent,fg_color=CARD,corner_radius=8,
                border_width=1,border_color=BD)
            f.grid(row=0,column=col,padx=8,pady=10,sticky="nsew")
            ctk.CTkLabel(f,text=title,font=("Segoe UI",12,"bold"),
                text_color=AC).pack(pady=(10,6),anchor="w",padx=12)
            return f
        # Timeframe + SMA
        tf=card(0,"Candle Timeframe")
        for v,l in [("5s","5 Seconds (WS)"),("1","1 Minute"),
                    ("5","5 Minutes"),("15","15 Minutes")]:
            ctk.CTkRadioButton(tf,text=l,variable=self.tf_var,value=v,
                font=FONT).pack(anchor="w",padx=20,pady=3)
        ctk.CTkFrame(tf,fg_color=BD,height=1).pack(fill="x",padx=12,pady=6)
        ctk.CTkLabel(tf,text="SMA Smoothing Period",font=FONT,
            text_color=TD).pack(anchor="w",padx=20)
        sr=ctk.CTkFrame(tf,fg_color="transparent"); sr.pack(anchor="w",padx=20,pady=(3,10))
        ctk.CTkEntry(sr,textvariable=self.sma_var,width=60,justify="center",
            font=FONT).pack(side="left")
        ctk.CTkLabel(sr,text=" (1=standard HA)",font=("Segoe UI",10,"bold"),
            text_color=GY).pack(side="left",padx=4)
        # Quantities
        qf=card(1,"Quantity per Trade")
        r0=ctk.CTkFrame(qf,fg_color="transparent"); r0.pack(fill="x",padx=12,pady=3)
        ctk.CTkLabel(r0,text="Default NSE qty:",width=130,anchor="w",font=FONT,
            text_color=TD).pack(side="left")
        ctk.CTkEntry(r0,textvariable=self.default_qty_v,width=60,justify="center",
            font=FONT).pack(side="left",padx=4)
        ctk.CTkFrame(qf,fg_color=BD,height=1).pack(fill="x",padx=12,pady=4)
        for lbl,var in [("GOLDTEN:",self.gold_v),("SILVERMICRO:",self.silv_v),
                        ("CRUDEOILM:",self.crude_v),("ZINCMINI:",self.zinc_v),
                        ("GOLDPETAL:",self.gp_v)]:
            r=ctk.CTkFrame(qf,fg_color="transparent"); r.pack(fill="x",padx=12,pady=2)
            ctk.CTkLabel(r,text=lbl,width=130,anchor="w",font=FONT,
                text_color=TD).pack(side="left")
            ctk.CTkEntry(r,textvariable=var,width=60,justify="center",
                font=FONT).pack(side="left",padx=4)
            ctk.CTkLabel(r,text="lots",font=("Segoe UI",10,"bold"),
                text_color=GY).pack(side="left")
        # Sessions
        sf=card(2,"Session & Mode")
        ctk.CTkSwitch(sf,text="Paper Mode (safe)",variable=self.paper_var,
            font=FONT,button_color=AC,progress_color=ACL).pack(anchor="w",padx=12,pady=6)
        ctk.CTkFrame(sf,fg_color=BD,height=1).pack(fill="x",padx=12,pady=4)
        ctk.CTkLabel(sf,text="Auto Square-off Time",font=FONT,
            text_color=TD).pack(anchor="w",padx=12,pady=(4,2))
        for lbl2,var2 in [("NSE/NIFTY:",self.nse_sq_var),("MCX:",self.mcx_sq_var)]:
            r2=ctk.CTkFrame(sf,fg_color="transparent"); r2.pack(fill="x",padx=12,pady=2)
            ctk.CTkLabel(r2,text=lbl2,width=80,anchor="w",font=FONT,
                text_color=TD).pack(side="left")
            ctk.CTkEntry(r2,textvariable=var2,width=70,justify="center",
                font=FONT).pack(side="left",padx=4)
        # Controls
        cf=card(3,"Control")
        self._lbl_status=ctk.CTkLabel(cf,text="Pre-loading...",
            font=FONT,text_color=TD,wraplength=190)
        self._lbl_status.pack(padx=12,pady=6)
        self._btn_start=ctk.CTkButton(cf,text="▶  START",
            font=("Segoe UI",13,"bold"),fg_color=GR,hover_color="#047857",
            command=self._start,height=38)
        self._btn_start.pack(fill="x",padx=12,pady=4)
        self._btn_stop=ctk.CTkButton(cf,text="■  STOP",
            font=("Segoe UI",13,"bold"),fg_color=RD,hover_color="#991B1B",
            command=self._stop,state="disabled",height=38)
        self._btn_stop.pack(fill="x",padx=12,pady=4)
        ctk.CTkButton(cf,text="⬛  Square Off All",font=FONT,
            fg_color="#374151",hover_color="#111827",
            command=lambda:threading.Thread(target=self.engine.sq_all,daemon=True).start()
            if self.engine else None,height=30).pack(fill="x",padx=12,pady=4)

    def _build_live(self,parent):
        parent.configure(fg_color=BG)
        # Toolbar
        tb=ctk.CTkFrame(parent,fg_color=CARD,corner_radius=6,
            border_width=1,border_color=BD,height=34)
        tb.pack(fill="x",padx=6,pady=(6,2)); tb.pack_propagate(False)
        self._lbl_sel=ctk.CTkLabel(tb,text="Ready — instruments will appear after pre-load",
            font=FONT,text_color=TD)
        self._lbl_sel.pack(side="left",padx=10)
        self._lbl_next=ctk.CTkLabel(tb,text="Next candle: --",
            font=FONT,text_color=TD)
        self._lbl_next.pack(side="right",padx=10)
        # Summary
        self._lbl_summary=ctk.CTkLabel(parent,text="",
            font=("Segoe UI",11,"bold"),text_color=TX,fg_color=CARD,corner_radius=4)
        self._lbl_summary.pack(fill="x",padx=6,pady=(1,2))
        # Treeview (single fast widget for all 178 rows)
        tree_frame=tk.Frame(parent,bg=BG)
        tree_frame.pack(fill="both",expand=True,padx=6,pady=2)
        style=ttk.Style()
        style.theme_use("clam")
        style.configure("Inst.Treeview",
            background=BG, foreground=TX, fieldbackground=BG,
            font=("Segoe UI",10,"bold"), rowheight=24, borderwidth=0)
        style.configure("Inst.Treeview.Heading",
            background=HBG, foreground="white",
            font=("Segoe UI",10,"bold"), borderwidth=0)
        style.map("Inst.Treeview",background=[("selected","#DBEAFE")],
                  foreground=[("selected",TX)])
        self._tree=ttk.Treeview(tree_frame,columns=TREE_COLS,show="headings",
            style="Inst.Treeview",selectmode="browse")
        for col,w in zip(TREE_COLS,COL_W):
            self._tree.heading(col,text=col)
            self._tree.column(col,width=w,minwidth=30,anchor="center")
        vsb=ttk.Scrollbar(tree_frame,orient="vertical",command=self._tree.yview)
        self._tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side="right",fill="y")
        self._tree.pack(fill="both",expand=True)
        self._tree.tag_configure("green",foreground=GR)
        self._tree.tag_configure("red",foreground=RD)
        self._tree.tag_configure("odd",background="#F8FAFC")
        self._tree.tag_configure("even",background=BG)
        self._tree_ids:Dict[str,str]={}   # name → iid

    def _build_log(self,parent):
        parent.configure(fg_color=BG)
        tb=ctk.CTkFrame(parent,fg_color=CARD,corner_radius=6,height=30)
        tb.pack(fill="x",padx=6,pady=(6,2)); tb.pack_propagate(False)
        ctk.CTkButton(tb,text="Clear",width=70,height=22,font=FONT,
            fg_color=BD,text_color=TX,hover_color="#E5E7EB",
            command=lambda:self._log_box.delete("1.0","end")).pack(
            side="right",padx=6,pady=4)
        self._log_box=ctk.CTkTextbox(parent,font=("Consolas",10,"bold"),
            fg_color=CARD,text_color=TX,border_width=1,border_color=BD)
        self._log_box.pack(fill="both",expand=True,padx=6,pady=(2,6))

    # ── Logging ──────────────────────────────────────────────────────────────
    def _log(self,msg:str):
        ts=datetime.now().strftime("%H:%M:%S")
        self._log_q.put(f"[{ts}] {msg}\n")

    def _flush_log(self):
        lines=[]
        try:
            while True: lines.append(self._log_q.get_nowait())
        except queue.Empty: pass
        if lines:
            self._log_box.insert("end","".join(lines))
            self._log_box.see("end")

    def _set_status(self,msg,color=TD):
        self._lbl_status.configure(text=msg,text_color=color)

    # ── Preload ──────────────────────────────────────────────────────────────
    def _preload(self):
        threading.Thread(target=self._do_preload,daemon=True).start()

    def _do_preload(self):
        self._log("[AUTO] Pre-loading instruments...")
        try:
            rows=load_master(self._log)
            self._master_rows=rows
            insts=self._resolve(rows)
            # Switch to live tab and build tree on main thread
            self.after(0,lambda: self._on_preloaded(insts))
        except Exception as e:
            self._log(f"[PRE-LOAD ERR] {e}")

    def _resolve(self,rows):
        insts=[]
        # MCX
        for sym,v in [("GOLDTEN",self.gold_v),("SILVERMICRO",self.silv_v),
                      ("CRUDEOILM",self.crude_v),("ZINCMINI",self.zinc_v),
                      ("GOLDPETAL",self.gp_v)]:
            m=resolve_mcx(rows,sym,self._log)
            if not m: continue
            mult=MCX_LOT_MULT.get(sym,1)
            st=InstrState(sym,m["sid"],"MCX_COMM",is_mcx=True,lot_mult=mult,qty=v.get())
            insts.append(st)
        # NSE
        self._log("  Building NSE index...")
        nse_idx=build_nse_index(rows)
        self._log(f"  NSE index: {len(nse_idx)} symbols")
        dq=self.default_qty_v.get(); found=0; missing=[]
        for sym in NSE_STOCKS:
            sid=nse_idx.get(sym.upper())
            if not sid: missing.append(sym); continue
            if sym not in self._stock_qty_vars:
                self._stock_qty_vars[sym]=ctk.IntVar(value=dq)
            insts.append(InstrState(sym,sid,"NSE_EQ",qty=self._stock_qty_vars[sym].get()))
            found+=1
        self._log(f"  ✓ {found} NSE  ✗ {len(missing)} not found")
        if missing: self._log(f"  Missing: {', '.join(missing[:10])}" + (f"...+{len(missing)-10}" if len(missing)>10 else ""))
        self._log(f"  {len(insts)} instruments ready.")
        return insts

    def _on_preloaded(self,insts):
        self.instruments=insts
        self._populate_tree()
        self._nb.set("Live Strategy")
        self._set_status(f"{len(insts)} instruments loaded — ready to START",GR)
        self._log(f"[AUTO] {len(insts)} instruments loaded — ready to START")

    def _populate_tree(self):
        """Populate ttk.Treeview — instant for 200+ rows."""
        for iid in self._tree.get_children(): self._tree.delete(iid)
        self._tree_ids.clear()
        for i,st in enumerate(self.instruments):
            tag="even" if i%2==0 else "odd"
            seg_label="MCX" if st.is_mcx else "NSE"
            iid=self._tree.insert("","end",values=(
                st.name,seg_label,"-","-","-","-",
                "FLAT","-",str(st.qty),"-","-","-","Ready"),tags=(tag,))
            self._tree_ids[st.name]=iid

    # ── Start / Stop ─────────────────────────────────────────────────────────
    def _start(self):
        if self._running: return
        if not self.instruments:
            self._set_status("No instruments loaded",RD); return
        cid,tok=load_token()
        if not cid or not tok:
            self._set_status("No token found",RD); return
        tf=self.tf_var.get()
        iv=5 if tf=="5s" else int(tf)*60
        sma=max(1,self.sma_var.get())
        for st in self.instruments:
            if not st.is_mcx and st.name in self._stock_qty_vars:
                st.qty=self._stock_qty_vars[st.name].get()
        self.engine=Engine(cid,tok,self.instruments,iv,sma,
            self.paper_var.get(),self.nse_sq_var.get(),self.mcx_sq_var.get(),
            log_fn=self._log)
        self.engine.start()
        self._running=True
        self._btn_start.configure(state="disabled",text="RUNNING ●")
        self._btn_stop.configure(state="normal")
        mode="PAPER" if self.paper_var.get() else "LIVE"
        self._lbl_mode.configure(text=mode,
            text_color="#FDE68A" if mode=="PAPER" else RD)
        tf_d=tf if tf=="5s" else f"{tf}m"
        self._set_status(f"Running | {tf_d}(WS) | {len(self.instruments)} insts | {mode}",GR)
        self._nb.set("Live Strategy")

    def _stop(self):
        if self.engine: self.engine.stop()
        self._running=False
        self._btn_start.configure(state="normal",text="▶  START")
        self._btn_stop.configure(state="disabled")
        self._set_status("Stopped",TD)

    # ── GUI tick (every 500ms) ────────────────────────────────────────────────
    def _tick(self):
        self._lbl_clk.configure(text=datetime.now().strftime("%d %b %Y  %H:%M:%S"))
        self._flush_log()   # batch log flush — no per-message GUI updates
        if self.engine:
            ws=self.engine.ws_status
            wsc=GR if "connected" in ws.lower() else (RD if "error" in ws.lower() else GY)
            self._lbl_ws.configure(text=f"WS: {'✓' if 'connected' in ws.lower() else '✕'} | ticks:{self.engine._feed.ticks}",
                text_color=wsc)
            self._lbl_next.configure(text=f"Next: {self.engine.next_candle}")
            self._update_tree()
        self.after(500,self._tick)

    def _update_tree(self):
        """Update tree rows in one pass — no widget recreation."""
        total_pnl=0.0; long_c=short_c=flat_c=0
        for st in self.instruments:
            iid=self._tree_ids.get(st.name)
            if not iid: continue
            pnl=st.pnl; total_pnl+=pnl
            if st.position=="LONG": long_c+=1
            elif st.position=="SHORT": short_c+=1
            else: flat_c+=1
            # Tick age
            age=time.time()-st.last_tick if st.last_tick>0 else 9999
            tick_s="●" if age<5 else f"○{int(age)}s" if age<30 else "✕"
            col_sym={"GREEN":"▲GREEN","RED":"▼RED","DOJI":"—DOJI"}.get(st.color,"-")
            pnl_s=f"₹{pnl:+.2f}" if st.position!="FLAT" else "-"
            tag=("green",) if st.position=="LONG" else (("red",) if st.position=="SHORT" else ())
            self._tree.item(iid,values=(
                st.name,
                "MCX" if st.is_mcx else "NSE",
                f"{st.ha_open:.2f}" if st.ha_open else "-",
                f"{st.ha_close:.2f}" if st.ha_close else "-",
                col_sym, st.signal, st.position,
                f"{st.entry:.2f}" if st.entry else "-",
                str(st.qty),
                f"{st.ltp:.2f}" if st.ltp else "-",
                pnl_s,
                st.bar_time,
                f"{st.status} {tick_s}"),
            tags=tag if tag else (("even" if self.instruments.index(st)%2==0 else "odd"),))
        pnl_c=GR if total_pnl>0 else (RD if total_pnl<0 else GOLD)
        self._lbl_pnl.configure(text=f"Net P&L: ₹{total_pnl:+,.2f}",text_color=pnl_c)
        self._lbl_summary.configure(
            text=f"  ↑LONG: {long_c}   ↓SHORT: {short_c}   FLAT: {flat_c}   |   P&L: ₹{total_pnl:+,.2f}")

# ─────────────────────────────────────────────────────────────────────────────
#  ENTRY
# ─────────────────────────────────────────────────────────────────────────────
def main():
    try:
        App().mainloop()
    except Exception:
        import traceback
        ctk.CTk().withdraw()
        import tkinter.messagebox as mb
        mb.showerror("Balfund HA Trader — Error",
                     f"Crash:\n\n{traceback.format_exc()}")

if __name__=="__main__":
    main()
