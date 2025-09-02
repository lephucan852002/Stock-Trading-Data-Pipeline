from fastapi import FastAPI, HTTPException
import os
import oracledb

app = FastAPI()

DSN = os.getenv('ORACLE_DSN', 'oracle-db:1521/XEPDB1')
USER = os.getenv('ORACLE_USER', 'APP')
PWD = os.getenv('ORACLE_PWD', 'app_pwd')

def get_conn():
    # Sử dụng keyword arguments cho oracledb mới
    return oracledb.connect(user=USER, password=PWD, dsn=DSN)

@app.get("/health")
def health():
    return {"status":"ok"}

@app.post("/order")
def create_order(payload: dict):
    conn = None
    cur = None
    try:
        conn = get_conn()
        cur = conn.cursor()
        out = cur.var(int)
        cur.callproc(
            'pkg_order_management.place_order',
            [
                payload['customer_id'],
                payload['symbol'],
                payload['side'],
                payload['quantity'],
                payload['price'],
                out
            ]
        )
        conn.commit()
        return {"order_id": out.getvalue()}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    finally:
        if cur: cur.close()
        if conn: conn.close()
