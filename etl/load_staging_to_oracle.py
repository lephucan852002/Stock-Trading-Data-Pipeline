import os, glob
import pandas as pd
import oracledb

# Kết nối Oracle
DSN = os.getenv('ORACLE_DSN','oracle-db:1521/XEPDB1')
USER = os.getenv('ORACLE_USER','APP')
PWD = os.getenv('ORACLE_PWD','app_pwd')
IN_DIR = "/data/historical"

conn = oracledb.connect(user=USER, password=PWD, dsn=DSN)
cur = conn.cursor()

cur.execute("TRUNCATE TABLE stg_historical_price")
conn.commit()


files = glob.glob(os.path.join(IN_DIR, "*.csv"))
for f in files:
    print("📥 Loading", f)
    df = pd.read_csv(f, parse_dates=['date'])

    # Đảm bảo đúng tên cột
    expected_cols = ['date','Symbol','open','high','low','close','adj_close','volume']
    if not all(c in df.columns for c in expected_cols):
        raise ValueError(f"❌ File {f} không đúng schema, cột hiện tại: {list(df.columns)}")

    # Chuẩn hóa kiểu dữ liệu
    df['Symbol'] = df['Symbol'].astype(str)
    df['open'] = pd.to_numeric(df['open'], errors='coerce')
    df['high'] = pd.to_numeric(df['high'], errors='coerce')
    df['low'] = pd.to_numeric(df['low'], errors='coerce')
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df['adj_close'] = pd.to_numeric(df['adj_close'], errors='coerce')
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(int)

    # Loại bỏ dòng lỗi (NaN do coerce)
    df = df.dropna(subset=['open','high','low','close','adj_close'])

    rows = [
        (
            r['Symbol'],
            r['date'].date(),
            float(r['open']),
            float(r['high']),
            float(r['low']),
            float(r['close']),
            float(r['adj_close']),
            int(r['volume'])
        )
        for _, r in df.iterrows()
    ]

    if rows:
        cur.executemany("""
            INSERT INTO stg_historical_price
            (symbol, trade_date, open_price, high_price, low_price, close_price, adj_close, volume)
            VALUES (:1,:2,:3,:4,:5,:6,:7,:8)
        """, rows)
        conn.commit()
        print(f"✅ Inserted {len(rows)} rows from {os.path.basename(f)}")
    else:
        print(f"⚠️ Không có dòng hợp lệ trong {os.path.basename(f)}")

cur.close()
conn.close()
