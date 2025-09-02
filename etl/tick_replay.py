# etl/tick_replay.py
import time, random, json, os
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

def create_producer(retries=30, wait=2):
    for i in range(retries):
        try:
            p = KafkaProducer(bootstrap_servers=KAFKA,
                              value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            return p
        except NoBrokersAvailable:
            print(f"[tick_replay] Kafka not ready, retry {i+1}/{retries} sleeping {wait}s")
            time.sleep(wait)
    raise RuntimeError("Cannot connect to Kafka after retries")

producer = create_producer()
symbols = ["AAPL","MSFT","GOOG","AMZN","TSLA"]
print("[tick_replay] started, producing to", KAFKA)
while True:
    for sym in symbols:
        base = 100 + random.random()*200
        ts = time.time()
        for i in range(random.randint(5,30)):
            tick = {'symbol': sym, 'ts': ts + i*0.01, 'price': round(base + random.uniform(-0.5,0.5), 4), 'size': random.randint(1,500)}
            try:
                producer.send('market_ticks', key=sym.encode('utf-8'), value=tick)
            except Exception as e:
                print("[tick_replay] send error:", e)
    try:
        producer.flush(timeout=10)
    except Exception as e:
        print("[tick_replay] flush error:", e)
    time.sleep(1)
