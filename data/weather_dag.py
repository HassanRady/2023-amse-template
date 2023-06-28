from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from weather_data_pipeline import start_pipeline


with DAG(dag_id="SAKI-WEATHER-DATA",
         start_date=datetime(2023, 5, 25),
         schedule="@daily",
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id="weather_data_getter",
        python_callable=start_pipeline)

task1
