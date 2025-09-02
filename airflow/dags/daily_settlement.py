from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {'owner':'airflow','retries':1,'retry_delay':timedelta(minutes=5)}

with DAG('daily_settlement', start_date=datetime(2024,1,1), schedule_interval='0 2 * * *', default_args=default_args, catchup=False) as dag:
    t1 = BashOperator(task_id='run_settlement', bash_command='python /app/../engine/settlement_worker.py')
