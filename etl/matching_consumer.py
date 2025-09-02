# etl/matching_consumer.py
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, os, time
import oracledb

KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')
DSN = os.getenv('ORACLE_DSN','oracle-db:1521/XEPDB1')
USER = os.getenv('ORACLE_USER','APP')
PWD = os.getenv('ORACLE_PWD','app_pwd')

def create_consumer(topic, retries=30, wait=2):
    for i in range(retries):
        try:
            c = KafkaConsumer(topic, bootstrap_servers=KAFKA, value_deserializer=lambda m: json.loads(m.decode('utf-8')), auto_offset_reset='earliest', enable_auto_commit=True)
            return c
        except NoBrokersAvailable:
            print(f"[matching_consumer] Kafka not ready, retry {i+1}/{retries} sleeping {wait}s")
            time.sleep(wait)
    raise RuntimeError("Cannot create KafkaConsumer after retries")

consumer = create_consumer('order_events')

# connect oracle with retry
for i in range(10):
    try:
        conn = oracledb.connect(user=USER, password=PWD, dsn=DSN)
        break
    except Exception as e:
        print("[matching_consumer] waiting for Oracle...", e)
        time.sleep(2)
else:
    raise RuntimeError("Cannot connect to Oracle DB")
cur = conn.cursor()

print("[matching_consumer] listening on order_events")
for msg in consumer:
    try:
        order = msg.value
        sym = order.get('symbol')
        if not sym:
            continue
        cur.callproc('pkg_matching_engine.match_orders_for_symbol', [sym])
        conn.commit()
        print("[matching_consumer] matched symbol", sym)
    except Exception as e:
        print("[matching_consumer] match error:", e)
        time.sleep(1)
