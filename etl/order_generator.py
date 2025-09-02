import requests, random, time, os

# Endpoint nhận order, mặc định localhost
API = os.getenv('ORDER_API','http://localhost:8080/order')

# Danh sách symbol mô phỏng
symbols = ["AAPL","MSFT","GOOG","AMZN","TSLA"]

# Loop liên tục sinh order
while True:
    payload = {
        "customer_id": random.randint(1001,1010),
        "symbol": random.choice(symbols),
        "side": random.choice(["BUY","SELL"]),
        "quantity": random.randint(1,200),
        "price": round(random.uniform(80,200),2)
    }

    try:
        r = requests.post(API, json=payload, timeout=5)
        print("Sent", payload, "->", r.status_code, r.text)
    except Exception as e:
        print("API err", e)

    time.sleep(0.5)  # 0.5 giây/sent 1 order, tăng tốc hoặc giảm tùy test
