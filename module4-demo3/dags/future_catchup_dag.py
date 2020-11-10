from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2021, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="future_catchup_dag",
         schedule_interval="@daily",
         default_args=default_args,
         catchup=True) as dag:
    task_1 = DummyOperator(task_id="task_1")
    task_2 = DummyOperator(task_id="task_2")
    task_1 >> task_2

