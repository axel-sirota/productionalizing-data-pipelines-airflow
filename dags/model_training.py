from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 10, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "axel.sirota@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


with DAG(dag_id="model_retraining", schedule_interval="@daily",
         default_args=default_args, catchup=False) as dag:

    # This file could come in S3 from our ecommerce application
    is_new_data_available = FileSensor(
        task_id="is_new_data_available",
        fs_conn_id="data_path",
        filepath="data.csv",
        poke_interval=5,
        timeout=20
    )

    saving_file = BashOperator(
        task_id="saving_file",
        bash_command="""
            hdfs dfs -mkdir -p /data && \
            hdfs dfs -put -f ${AIRFLOW_HOME}/data/data.csv /data
            """
    )

    model_training = SparkSubmitOperator(
        task_id="model_training",
        conn_id="spark_conn",
        application="spark-scripts/model_training.py",
        verbose=False
    )

    # Now could come an upload to S3 of the model or a deploy step

    is_new_data_available >> saving_file
    is_new_data_available >> model_training
