import os, json, time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import oracledb
from collections import defaultdict, deque
from statistics import stdev

# ========================
# Config
# ========================
KAFKA_TOPIC = 'market_ticks'
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')
ORACLE_DSN  = os.getenv('ORACLE_DSN','oracle-db:1521/XEPDB1')
ORACLE_USER = os.getenv('ORACLE_USER','APP')
ORACLE_PWD  = os.getenv('ORACLE_PWD','app_pwd')

WINDOW_SIZE = 200        # sliding window size per symbol
BATCH_SIZE  = 50         # số tick sau mới compute risk
FLUSH_SIZE  = 100        # số symbol risk để flush 1 lần

# ========================
# Kafka consumer
# ========================
def create_consumer(topic):
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True
            )
            return consumer
        except KafkaError as e:
            print("[RISK_ENGINE] Kafka not ready, retrying in 2s...", e)
            time.sleep(2)

# ========================
# Oracle connection with retry
# ========================
def get_conn(retries=10, wait=2):
    for i in range(retries):
        try:
            conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PWD, dsn=ORACLE_DSN)
            return conn
        except oracledb.DatabaseError as e:
            print(f"[RISK_ENGINE] Waiting for Oracle ({i+1}/{retries})...", e)
            time.sleep(wait)
    raise RuntimeError("Cannot connect to Oracle DB")

# ========================
# Risk calculation
# ========================
def compute_risk(prices):
    if len(prices) < 2:
        return 0.0
    return float(stdev(prices)) * 1000

# ========================
# Main loop
# ========================
def main():
    print("[RISK_ENGINE] Starting optimized risk engine...")
    consumer = create_consumer(KAFKA_TOPIC)
    conn = get_conn()
    cur = conn.cursor()
    print("[RISK_ENGINE] Connected to Oracle, listening to market_ticks...")

    # buffer: symbol -> deque of prices
    window = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
    # batch risk: symbol -> (ts, score)
    batch_risk = {}

    for msg in consumer:
        try:
            v = msg.value
            sym = v.get('symbol')
            price = v.get('price')
            if not sym or price is None:
                continue

            # update sliding window
            window[sym].append(price)

            # compute risk per batch
            if len(window[sym]) % BATCH_SIZE == 0:
                score = compute_risk(list(window[sym]))
                ts = time.strftime("%Y-%m-%d %H:%M:%S")
                batch_risk[sym] = (ts, score)

            # flush batch to DB when enough symbols collected
            if len(batch_risk) >= FLUSH_SIZE:
                try:
                    for s, (ts, score) in batch_risk.items():
                        cur.execute("""
                            MERGE INTO fact_symbol_risk tgt
                            USING (SELECT :1 symbol, TO_DATE(:2,'YYYY-MM-DD HH24:MI:SS') dt FROM dual) src
                            ON (tgt.symbol=src.symbol AND TRUNC(tgt.risk_ts)=TRUNC(src.dt))
                            WHEN MATCHED THEN UPDATE SET risk_score=:3, model_version='v1'
                            WHEN NOT MATCHED THEN 
                              INSERT(symbol, risk_ts, risk_score, model_version)
                              VALUES(:4, TO_DATE(:5,'YYYY-MM-DD HH24:MI:SS'), :6, 'v1')
                        """, [s, ts, score, s, ts, score])
                    conn.commit()
                    print(f"[RISK_ENGINE] Flushed {len(batch_risk)} symbols to DB")
                    batch_risk.clear()
                except Exception as e:
                    print("[RISK_ENGINE] DB write error:", e)
                    conn.rollback()

        except Exception as e:
            print("[RISK_ENGINE] Processing error:", e)
            time.sleep(1)

if __name__ == "__main__":
    main()
