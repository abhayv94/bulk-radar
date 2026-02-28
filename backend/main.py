from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, timedelta
import httpx
import csv
import io

app = FastAPI(title="Bulk & Block Deals Radar")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Deal(BaseModel):
    id: int
    date_time: str
    symbol: str
    exchange: str
    deal_type: str
    side: str
    quantity: int
    price: float
    value_cr: float
    equity_pct: float
    buyer: str
    seller: str

class Signal(BaseModel):
    id: int
    symbol: str
    signal_type: str
    description: str
    advice: str
    score: float
    date_time: str

def get_last_trading_date() -> date:
    d = date.today()
    if d.weekday() == 5:
        d -= timedelta(days=1)
    elif d.weekday() == 6:
        d -= timedelta(days=2)
    return d

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0 Safari/537.36",
    "Referer": "https://www.nseindia.com/",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

async def fetch_csv(url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True, headers=HEADERS) as client:
            await client.get("https://www.nseindia.com/")
            r = await client.get(url)
            if r.status_code == 200:
                return r.text
    except Exception as e:
        print(f"CSV fetch error: {e}")
    return ""

async def get_all_deals() -> List[Deal]:
    d = get_last_trading_date()
    dt_str = d.strftime("%d-%m-%Y")
    deals = []
    idx = 1
    bulk_url = f"https://archives.nseindia.com/archives/equities/bulk/bulk{d.strftime('%d%m%Y')}.csv"
    bulk_csv = await fetch_csv(bulk_url)
    if bulk_csv:
        reader = csv.DictReader(io.StringIO(bulk_csv))
        for row in reader:
            try:
                symbol = str(row.get("Symbol", row.get("SYMBOL", ""))).strip().upper()
                client = str(row.get("Client Name", row.get("CLIENT NAME", ""))).strip().upper()
                trade_type = str(row.get("Buy/Sell", row.get("BUY/SELL", ""))).strip().upper()
                qty = int(str(row.get("Quantity Traded", row.get("QTY.", "0"))).replace(",", "").strip() or 0)
                price = float(str(row.get("Wt. Avg. Price", row.get("PRICE", "0"))).replace(",", "").strip() or 0)
                if qty == 0 or price == 0 or not symbol:
                    continue
                value_cr = round((qty * price) / 1e7, 2)
                side = "sell" if "S" in trade_type else "buy"
                deals.append(Deal(
                    id=idx, date_time=dt_str, symbol=symbol,
                    exchange="NSE", deal_type="bulk", side=side,
                    quantity=qty, price=price, value_cr=value_cr,
                    equity_pct=0.0,
                    buyer=client if side == "buy" else "--",
                    seller=client if side == "sell" else "--",
                ))
                idx += 1
            except Exception as e:
                print(f"Row error: {e}")
                continue
    print(f"Total deals: {len(deals)} for {dt_str}")
    return deals

PROMOTER_KW = ["PROMOTER","FOUNDER","BLACKSTONE","KEDAARA","WARBURG","SEQUOIA","KKR","CARLYLE","BAIN","TPG"]
MF_FII_KW = ["FUND","MUTUAL","MF","FII","FPI","ETF","HDFC","SBI","ICICI","NIPPON","KOTAK","AXIS","DSP","MOTILAL","MIRAE","FRANKLIN","VANGUARD","BLACKROCK","MORGAN","GOLDMAN","JPMORGAN","NOMURA","CITIBANK"]

def classify(name: str) -> str:
    n = name.upper()
    for k in PROMOTER_KW:
        if k in n: return "PROMOTER/PE"
    for k in MF_FII_KW:
        if k in n: return "MF/FII"
    return "HNI/OTHER"

def make_signals(deals: List[Deal]) -> List[Signal]:
    from collections import defaultdict
    groups = defaultdict(list)
    for d in deals:
        groups[d.symbol].append(d)
    sigs = []
    sid = 1
    for sym, grp in groups.items():
        buys = [d for d in grp if d.side == "buy"]
        sells = [d for d in grp if d.side == "sell"]
        tbv = sum(d.value_cr for d in buys)
        tsv = sum(d.value_cr for d in sells)
        dt = grp[0].date_time
        for d in buys:
            if d.value_cr >= 10:
                ent = classify(d.buyer)
                sigs.append(Signal(id=sid, symbol=sym, signal_type="fresh_big_buy",
                    description=f"Bulk BUY Rs{d.value_cr:.1f} Cr by {d.buyer} [{ent}].",
                    advice="BUY - Strong institutional entry. Consider buying on dips.",
                    score=min(0.95, 0.6 + d.value_cr/500), date_time=dt))
                sid += 1
        if len(buys) >= 2:
            sigs.append(Signal(id=sid, symbol=sym, signal_type="repeated_accumulation",
                description=f"{len(buys)} bulk BUYs totalling Rs{tbv:.1f} Cr today.",
                advice="STRONG BUY - Multiple institutions buying.",
                score=0.9, date_time=dt))
            sid += 1
        for d in sells:
            ent = classify(d.seller)
            if ent == "PROMOTER/PE" and d.value_cr >= 10:
                sigs.append(Signal(id=sid, symbol=sym, signal_type="promoter_exit",
                    description=f"Bulk SELL Rs{d.value_cr:.1f} Cr by {d.seller} [PROMOTER/PE].",
                    advice="BOOK PROFIT - Promoter/PE exiting. Best time to sell.",
                    score=0.85, date_time=dt))
                sid += 1
            elif ent == "MF/FII" and d.value_cr >= 10:
                sigs.append(Signal(id=sid, symbol=sym, signal_type="mf_fii_exit",
                    description=f"MF/FII Bulk SELL Rs{d.value_cr:.1f} Cr by {d.seller}.",
                    advice="CAUTION - Institution selling. Consider partial profit booking.",
                    score=0.75, date_time=dt))
                sid += 1
        if tbv >= 10 and tsv >= 10:
            sigs.append(Signal(id=sid, symbol=sym, signal_type="mixed_activity",
                description=f"Both BUY Rs{tbv:.1f} Cr and SELL Rs{tsv:.1f} Cr today.",
                advice="HOLD - Mixed signals. Wait for clear direction.",
                score=0.6, date_time=dt))
            sid += 1
    sigs.sort(key=lambda s: s.score, reverse=True)
    return sigs

@app.get("/deals", response_model=List[Deal])
async def get_deals(side: Optional[str]=None, min_value_cr: Optional[float]=5.0, deal_type: Optional[str]=None):
    all_deals = await get_all_deals()
    if side:
        all_deals = [d for d in all_deals if d.side == side.lower()]
    if min_value_cr:
        all_deals = [d for d in all_deals if d.value_cr >= min_value_cr]
    if deal_type:
        all_deals = [d for d in all_deals if d.deal_type == deal_type.lower()]
    all_deals.sort(key=lambda d: d.value_cr, reverse=True)
    return all_deals

@app.get("/signals", response_model=List[Signal])
async def get_signals(min_value_cr: Optional[float]=5.0):
    all_deals = await get_all_deals()
    if min_value_cr:
        all_deals = [d for d in all_deals if d.value_cr >= min_value_cr]
    return make_signals(all_deals)

@app.get("/summary")
async def get_summary():
    all_deals = await get_all_deals()
    tbv = sum(d.value_cr for d in all_deals if d.side == "buy")
    tsv = sum(d.value_cr for d in all_deals if d.side == "sell")
    if tbv > tsv * 1.3:
        mood = "BULLISH - Heavy institutional buying. Good time to enter fresh positions."
    elif tsv > tbv * 1.3:
        mood = "BEARISH - Heavy institutional selling. Book profits, avoid fresh entry."
    else:
        mood = "NEUTRAL - Mixed activity. Wait for clearer direction before investing."
    d = get_last_trading_date()
    return {
        "date": d.strftime("%d-%m-%Y"),
        "total_deals": len(all_deals),
        "buy_deals": len([x for x in all_deals if x.side=="buy"]),
        "sell_deals": len([x for x in all_deals if x.side=="sell"]),
        "total_buy_value_cr": round(tbv, 2),
        "total_sell_value_cr": round(tsv, 2),
        "market_mood": mood,
    }
