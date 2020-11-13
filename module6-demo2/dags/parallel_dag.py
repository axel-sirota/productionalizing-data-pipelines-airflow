from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import BranchPythonOperator
import numpy as np

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="parallel_dag",
         schedule_interval="@daily",
         start_date=datetime(2020, 11, 1),
         default_args=default_args,
         catchup=False) as dag:
    tasks = [DummyOperator(task_id=f"{i}") for i in range(10)]
    end = DummyOperator(task_id="none")
    tasks >> end
