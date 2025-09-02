from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, os, time
import oracledb
from datetime import datetime

# ---------- Config ----------
KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
DSN = os.getenv('ORACLE_DSN', 'oracle-db:1521/XEPDB1')
USER = os.getenv('ORACLE_USER', 'APP')
PWD = os.getenv('ORACLE_PWD', 'app_pwd')

# ---------- Kafka Consumer ----------
def create_consumer(topic, retries=30, wait=2):
    for i in range(retries):
        try:
            c = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            return c
        except NoBrokersAvailable:
            print(f"[analytics_consumer] Kafka not ready, retry {i+1}/{retries}, sleeping {wait}s")
            time.sleep(wait)
    raise RuntimeError("Cannot create KafkaConsumer after retries")

consumer = create_consumer('market_ticks')
print("[analytics_consumer] connected to", KAFKA)

# ---------- Oracle DB Connect ----------
for _ in range(10):
    try:
        conn = oracledb.connect(user=USER, password=PWD, dsn=DSN)
        break
    except Exception as e:
        print("[analytics_consumer] waiting for Oracle...", e)
        time.sleep(2)
else:
    raise RuntimeError("Cannot connect to Oracle DB")

cur = conn.cursor()

# ---------- Helpers ----------
buffer = {}  # key: (symbol, minute_ts), value: list of prices

def minute_floor(ts):
    dt = datetime.fromtimestamp(ts)
    return dt.replace(second=0, microsecond=0)

# ---------- Main Loop ----------
last_flush_minute = None

for msg in consumer:
    try:
        v = msg.value
        sym = v['symbol']
        ts = v['ts']
        price = v['price']
        minute_ts = minute_floor(ts)
        key = (sym, minute_ts)

        buffer.setdefault(key, []).append(price)

        # Flush at the start of a new minute
        if last_flush_minute is None:
            last_flush_minute = minute_ts

        if minute_ts > last_flush_minute:
            # Flush all complete minutes older than last_flush_minute
            flush_keys = [k for k in buffer.keys() if k[1] <= last_flush_minute]
            for (s, m_ts) in flush_keys:
                prices = buffer.pop((s, m_ts), [])
                if not prices:
                    continue

                open_p, close_p = prices[0], prices[-1]
                high_p, low_p = max(prices), min(prices)
                volume = len(prices)
                tick_count = len(prices)

                try:
                    # MERGE with 16 bind variables
                    cur.execute("""
                        MERGE INTO fact_tick_agg tgt
                        USING (SELECT :1 AS symbol, :2 AS minute_ts FROM dual) src
                        ON (tgt.symbol = src.symbol AND tgt.minute_ts = src.minute_ts)
                        WHEN MATCHED THEN 
                            UPDATE SET 
                                open_price=:3, high_price=:4, low_price=:5, close_price=:6, 
                                volume=:7, tick_count=:8
                        WHEN NOT MATCHED THEN 
                            INSERT (symbol, minute_ts, open_price, high_price, low_price, close_price, volume, tick_count)
                            VALUES (:9, :10, :11, :12, :13, :14, :15, :16)
                    """, [
                        s, m_ts,                        # :1, :2 for USING
                        open_p, high_p, low_p, close_p, volume, tick_count,  # :3-:8 for UPDATE
                        s, m_ts, open_p, high_p, low_p, close_p, volume, tick_count  # :9-:16 for INSERT
                    ])
                except Exception as e:
                    print("[analytics_consumer] DB upsert error:", e)
                    conn.rollback()
            conn.commit()  # Commit once per minute batch
            last_flush_minute = minute_ts

    except Exception as e:
        print("[analytics_consumer] processing error:", e)
