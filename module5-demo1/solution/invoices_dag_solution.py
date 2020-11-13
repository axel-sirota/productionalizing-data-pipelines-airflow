import json
import os
from datetime import datetime, timedelta

from airflow.contrib.operators.slack_webhook_operator import \
    SlackWebhookOperator
from sqlalchemy import create_engine

import pandas as pd
from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.hooks.base_hook import BaseHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2020, 11, 1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "axel.sirota@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

data_path = f'{json.loads(BaseHook.get_connection("data_path").get_extra()).get("path")}/data.csv'
transformed_path = f'{os.path.splitext(data_path)[0]}-transformed.csv'
slack_token = BaseHook.get_connection("slack_conn").password


def transform_data(ds, **kwargs):
    invoices_data = pd.read_csv(filepath_or_buffer=data_path,
                                sep=',',
                                header=0,
                                usecols=['StockCode', 'Quantity', 'InvoiceDate',
                                         'UnitPrice', 'CustomerID', 'Country'],
                                parse_dates=['InvoiceDate'],
                                index_col=0
                                )
    filtered_invoices_data = invoices_data[invoices_data['InvoiceDate'].dt.strftime("%Y-%m-%d") == f"{ds}"]
    filtered_invoices_data.to_csv(path_or_buf=transformed_path)


def store_in_db(*args, **kwargs):
    transformed_invoices = pd.read_csv(transformed_path)
    transformed_invoices.columns = [c.lower() for c in
                                    transformed_invoices.columns]  # postgres doesn't like capitals or spaces

    transformed_invoices.dropna(axis=0, how='any', inplace=True)
    engine = create_engine(
        'postgresql://airflow:airflow@postgres/pluralsight')

    transformed_invoices.to_sql("invoices",
                                engine,
                                if_exists='append',
                                chunksize=500,
                                index=False
                                )


with DAG(dag_id="invoices_dag_solution",
         schedule_interval="@daily",
         default_args=default_args,
         template_searchpath=[f"{os.environ['AIRFLOW_HOME']}"],
         catchup=False) as dag:
    # This file could come in S3 from our ecommerce application
    is_new_data_available = FileSensor(
        task_id="is_new_data_available",
        fs_conn_id="data_path",
        filepath="data.csv",
        poke_interval=5,
        timeout=20
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )

    create_table = PostgresOperator(
        task_id="create_table",
        sql='''CREATE TABLE IF NOT EXISTS invoices (
                stockcode VARCHAR(50) NOT NULL,
                quantity integer NOT NULL,
                invoicedate DATE NOT NULL,
                unitprice decimal NOT NULL,
                customerid integer NOT NULL,
                country VARCHAR (50) NOT NULL
                );''',
        postgres_conn_id='postgres',
        database='pluralsight'
    )

    save_into_db = PythonOperator(
        task_id='save_into_db',
        python_callable=store_in_db
    )

    create_report = PostgresOperator(
        task_id="create_report",
        sql=['exec_report.sql'],
        postgres_conn_id='postgres',
        database='pluralsight',
        autocommit=True
    )

    notify_data_science_team = SlackWebhookOperator(
        task_id='notify_data_science_team',
        http_conn_id='slack_conn',
        webhook_token=slack_token,
        message="Data Science Notification \n"
                "New Invoice Data from day {{ macros.ds_format(ds, '%Y-%m-%d', '%d-%m-%Y') }} is loaded into invoices table. \n "
                "Here is a celebration kitty: "
                "https://www.youtube.com/watch?v=J---aiyznGQ",
        username='airflow',
        icon_url='https://raw.githubusercontent.com/apache/'
                 'airflow/master/airflow/www/static/pin_100.png',
        dag=dag
    )

    # Now could come an upload to S3 of the model or a deploy step

    is_new_data_available >> transform_data
    transform_data >> create_table >> save_into_db
    save_into_db >> notify_data_science_team
    save_into_db >> create_report
