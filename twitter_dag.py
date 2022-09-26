from datetime import timedelta
from email.policy import default
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_airflow import run_twitter_etl


#airflow settings
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2020,11,8),
    'email':['jalilpablo@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

#creating DAG
dag = DAG(
    'twitter_dag',  #name
    default_args=default_args, #args
    description='Airflow test'
)

run_etl = PythonOperator(
    task_id='twitter_etl',
    python_callable=run_twitter_etl, #what to do
    dag=dag #in wich dag
)

run_etl