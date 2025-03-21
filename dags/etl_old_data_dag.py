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

def fetch_old_data():
    etc = EuroleagueDataConnector()
    etc.fetch_old_box_score_data()

def transform_old_data():
    print("Transforming old data...")

with DAG(
    dag_id='euroleague_etl_old_data',
    default_args=default_args,
    description='Daily ETL tasks for Euroleague data',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
)  as dag:

    task_fetch_old = PythonOperator(
        task_id='fetch_old_data',
        python_callable=fetch_old_data
    )

    task_transform_old = PythonOperator(
        task_id='transform_old_data',
        python_callable=transform_old_data
    )

    task_fetch_old >> task_transform_old