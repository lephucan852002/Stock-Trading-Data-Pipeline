import os, json, time
from kafka import KafkaConsumer
import oracledb

KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')
DSN = os.getenv('ORACLE_DSN','oracle-db:1521/XEPDB1')
USER = os.getenv('ORACLE_USER','APP')
PWD = os.getenv('ORACLE_PWD','app_pwd')

consumer = KafkaConsumer(
    'order_events',
    bootstrap_servers=KAFKA,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

def get_conn():
    # Sửa thành keyword arguments
    return oracledb.connect(user=USER, password=PWD, dsn=DSN)

def main():
    conn = get_conn()
    cur = conn.cursor()
    print("Matching engine started, listening to order_events...")

    for msg in consumer:
        try:
            order = msg.value
            sym = order.get('symbol')
            if not sym:
                continue
            print(f"[MATCH] calling matcher for {sym}")
            cur.callproc('pkg_matching_engine.match_orders_for_symbol', [sym])
            conn.commit()
        except Exception as e:
            print("Matching error:", e)
            time.sleep(1)

if __name__ == "__main__":
    main()
