from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import MongoOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="mongo_dag",
         schedule_interval="@hourly",
         default_args=default_args,
         catchup=False) as dag:
    task_1 = MongoOperator(
        task_id='mongo',
        mongo_conn_id='mongo',
        mongo_collection='pluralsight',
        mongo_database='pluralsight',
        mongo_query=None
    )
    task_2 = DummyOperator(task_id="task_2")
    task_1 >> task_2

