from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from ..etl import run_crypto_etl
default_args = {
    'owner' : 'nicoadmin',
    'depends_on_past' : False,
    'start_date' : '2022-07-07',
    'email' : ['nsgorlew@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    'crypto_etl_dag',
    default_args=default_args,
    description='CoinGecko API DAG'
)

def just_a_function():
    print("Getting Crypto data...")

run_etl = PythonOperator(
    task_id = 'crypto__etl',
    python_callable=run_crypto_etl,
    dag=dag
)

run_etl
