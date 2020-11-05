from datetime import datetime, timedelta

from airflow import DAG

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="invoices_dag",
         schedule_interval="@daily",
         default_args=default_args,
         catchup=False) as dag:
    None
