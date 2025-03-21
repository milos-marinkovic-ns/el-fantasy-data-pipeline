from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_pipeline.euroleague_data_connector import EuroleagueDataConnector


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def fetch_new_data():
    edc = EuroleagueDataConnector()
    edc.save_to_s3({})
    

def fetch_old_data():
    print("Fetching old data...")

def transform_new_data():
    print("Transforming new data...")

def transform_old_data():
    print("Transforming old data...")

with DAG(
    dag_id='euroleague_etl_daily',
    default_args=default_args,
    description='Daily ETL tasks for Euroleague data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task_fetch_new = PythonOperator(
        task_id='fetch_new_data',
        python_callable=fetch_new_data
    )
    
    task_transform_new = PythonOperator(
        task_id='transform_new_data',
        python_callable=transform_new_data
    )

    task_fetch_new >> task_transform_new