import os, time
import oracledb
from datetime import datetime

DSN = os.getenv('ORACLE_DSN','oracle-db:1521/XEPDB1')
USER = os.getenv('ORACLE_USER','APP')
PWD = os.getenv('ORACLE_PWD','app_pwd')

def get_conn():
    return oracledb.connect(user=USER, password=PWD, dsn=DSN)


def run_settlement_for(date_str):
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.callproc('pkg_settlement.run_daily_settlement', [date_str])
        conn.commit()
        print("Settlement done for", date_str)
    except Exception as e:
        print("Settlement error", e)
        conn.rollback()
    finally:
        cur.close(); conn.close()

if __name__=='__main__':
    from datetime import datetime
    run_settlement_for(datetime.utcnow().date())  # truyền datetime.date
