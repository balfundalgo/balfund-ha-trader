# Balfund HA + SMA(1) Strategy Trader

**Balfund Trading Private Limited**

Automated intraday trading strategy using Heikin Ashi candles on 21 NSE stocks + GOLDTEN & SILVERMICRO MCX futures via Dhan API.

---

## Strategy Logic

| Condition | Signal |
|-----------|--------|
| HA candle GREEN (Close > Open) | **BUY** (go long) |
| HA candle RED (Close < Open) | **SELL** (go short) |
| HA candle DOJI | Hold current position |

- **Always in market** after startup entry
- Reversal = close existing + open opposite
- Entry on startup from current HA bar color

---

## Files

| File | Purpose |
|------|---------|
| `token_generator.py` | GUI to generate Dhan access token |
| `ha_strategy_dhan.py` | Main terminal strategy |
| `dhan_ws_client.py` | Dhan WebSocket binary packet parser |
| `requirements.txt` | Python dependencies |

---

## How to Run (from source)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Generate token (run once per day)
python token_generator.py

# 3. Start strategy
python ha_strategy_dhan.py
```

---

## Download Windows EXEs

Go to **[Releases](../../releases)** → download the latest build:

- `Balfund_Token_Generator.exe` — run first, generates token
- `Balfund_HA_Trader.exe` — main strategy (run in terminal/cmd)

---

## Instruments

### NSE EQ (Intraday)
ULTRACEMCO · DIXON · APARINDS · BAJAJHLDNG · BAJAJ-AUTO · GILLETTE · APOLLOHOSP · LINDEINDIA · OFSS · POLYCAB · CRAFTSMAN · EICHERMOT · ATUL · AMBER · ABB · NAVINFLUOR · DIVISLAB · BRITANNIA · ALKEM · PERSISTENT · JKCEMENT

### MCX Futures
GOLDTEN (10g/lot) · SILVERMICRO (1000g/lot)

---

## Token Flow

```
token_generator.py
  → broadcasts to → http://localhost:5555/token
  → writes to     → C:\balfund_shared\dhan_token.json

ha_strategy_dhan.py
  → reads from → localhost:5555  (if generator is open)
  → fallback   → dhan_token.json
  → fallback   → .env (dhan_token_manager.py)
```

---

## Session Times

| Exchange | Session |
|----------|---------|
| NSE | 09:15 – 15:30 (auto sq-off 15:15 default) |
| MCX | 09:00 – 23:30 (auto sq-off 23:25 default) |

---

*Confidential — Balfund Trading Private Limited*
