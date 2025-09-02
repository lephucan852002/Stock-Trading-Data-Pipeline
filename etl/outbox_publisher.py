# etl/outbox_publisher.py
import oracledb, json, time, os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

DSN = os.getenv('ORACLE_DSN','oracle-db:1521/XEPDB1')
USER = os.getenv('ORACLE_USER','APP')
PWD = os.getenv('ORACLE_PWD','app_pwd')
KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS','kafka:9092')

# connect to oracle (with retry)
for i in range(10):
    try:
        conn = oracledb.connect(user=USER, password=PWD, dsn=DSN)
        break
    except Exception as e:
        print("[outbox_publisher] waiting for Oracle...", e)
        time.sleep(2)
else:
    raise RuntimeError("Cannot connect to Oracle")

cur = conn.cursor()

def create_producer(retries=30, wait=2):
    for i in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            return p
        except NoBrokersAvailable:
            print(f"[outbox_publisher] Kafka not ready, retry {i+1}/{retries} sleeping {wait}s")
            time.sleep(wait)
    raise RuntimeError("Cannot connect to Kafka after retries")

producer = create_producer()
print("[outbox_publisher] connected to Kafka at", KAFKA)

while True:
    try:
        cur.execute("SELECT event_id, event_type, payload FROM outbox_events WHERE dispatched='N' ORDER BY created_ts FETCH FIRST 100 ROWS ONLY")
        rows = cur.fetchall()
        if not rows:
            time.sleep(2)
            continue
        for event_id, event_type, payload in rows:
            try:
                msg = json.loads(payload)
                # fix: use Python startswith instead of SQL LIKE
                topic = 'order_events' if event_type.upper().startswith('ORDER') else 'trade_events'
                producer.send(topic, msg)
                cur.execute("UPDATE outbox_events SET dispatched='Y' WHERE event_id=:1", [event_id])
                conn.commit()
                print(f"[outbox_publisher] published event {event_id} -> {topic}")
            except Exception as e:
                print("[outbox_publisher] publish error for event", event_id, e)
                conn.rollback()
                time.sleep(1)
        producer.flush()
    except Exception as e:
        print("[outbox_publisher] main loop error:", e)
        time.sleep(2)
