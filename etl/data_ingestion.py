import yfinance as yf
import pandas as pd
import os

SYMS = ["AAPL","MSFT","GOOG","AMZN","TSLA","NVDA","META","BAC","JPM","V"]
OUT_DIR = "/data/historical"
os.makedirs(OUT_DIR, exist_ok=True)

def fetch(symbols, period="5y"):
    for s in symbols:
        print("Fetching", s)
        df = yf.download(s, period=period, progress=False, auto_adjust=False)
        if df.empty:
            print("No data for", s)
            continue
        df = df.reset_index()
        df['Symbol'] = s
        df = df.rename(columns={'Date':'date','Open':'open','High':'high','Low':'low','Close':'close','Adj Close':'adj_close','Volume':'volume'})
        df[['date','Symbol','open','high','low','close','adj_close','volume']].to_csv(f"{OUT_DIR}/{s}.csv", index=False)
        print("Saved", s)

if __name__ == "__main__":
    fetch(SYMS)
