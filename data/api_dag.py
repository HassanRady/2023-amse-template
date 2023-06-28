from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from pathlib import Path

from db_api_data_pipeline import start_api_pipeline


with DAG(dag_id="SAKI-DB-API",
         start_date=datetime(2023, 5, 25),
         schedule="@hourly",
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id="api_data_getter",
        python_callable=start_api_pipeline)

task1
