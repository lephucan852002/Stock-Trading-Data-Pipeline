from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.oracle.operators.oracle import OracleOperator
from datetime import datetime, timedelta

default_args = {'owner':'airflow','retries':1,'retry_delay':timedelta(minutes=5)}

with DAG('daily_historical_etl', start_date=datetime(2024,1,1), schedule_interval='@daily', default_args=default_args, catchup=False) as dag:
    t1 = BashOperator(task_id='fetch_historical', bash_command='python /app/data_ingestion.py')
    t2 = BashOperator(task_id='load_staging', bash_command='python /app/load_staging_to_oracle.py')
    t3 = OracleOperator(task_id='call_pkg_load', oracle_conn_id='oracle_default', sql="BEGIN pkg_data_pipeline.load_historical_prices(SYSDATE-1); END;")
    t1 >> t2 >> t3
