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


def select_tasks(*args, **context):
    ids = np.random.choice(10, size=3, replace=False)
    return [str(i) for i in ids]


with DAG(dag_id="non_parallel_dag",
         schedule_interval="@daily",
         start_date=datetime(2020, 11, 1),
         default_args=default_args,
         catchup=False) as dag:
    start = BranchPythonOperator(
        task_id="select_tasks",
        python_callable=select_tasks
    )
    tasks = [DummyOperator(task_id=f"{i}") for i in range(10)]
    end = DummyOperator(task_id="none", trigger_rule='one_success')
    start >> tasks >> end
