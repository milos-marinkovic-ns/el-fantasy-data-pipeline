from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from data_pipeline.euroleague_data_connector import EuroleagueDataConnector
from data_pipeline.euroleague_data_etl import EuroleagueDataETL


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_new_data():
    print("Fetching new data...")
    edc = EuroleagueDataConnector()
    edc.fetch_new_box_score_data()


def transform_new_data():
    print("Transforming new data...")
    etl = EuroleagueDataETL()
    etl.transform_new_box_score_data('euroleague-boxscore-data', 'data/boxscore_json')

with DAG(
    dag_id='euroleague_etl_new_data',
    default_args=default_args,
    description='ETL tasks for Euroleague data every 5 days',
    schedule_interval=timedelta(days=5),
    start_date=datetime(2025, 1, 1, 0, 0),
    catchup=False,
) as dag:

    task_fetch_new= PythonOperator(
        task_id='fetch_new_data',
        python_callable=fetch_new_data
    )

    task_transform_new = PythonOperator(
        task_id='transform_new_data',
        python_callable=transform_new_data
    )

    task_fetch_new >> task_transform_new