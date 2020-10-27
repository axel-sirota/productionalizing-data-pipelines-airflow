#!/usr/bin/env bash

# Create the user airflow in the HDFS
hdfs dfs -mkdir -p   /user/airflow/
hdfs dfs -chmod g+w  /user/airflow

export SPARK_DIST_CLASSPATH=$(hadoop classpath)
# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Initiliase the metadatabase
airflow resetdb -y
# shellcheck disable=SC2016
airflow connections --add --conn_id 'data_path' --conn_type File --conn_extra '{ "path" : "data" }'
airflow connections --add --conn_id 'spark_conn' --conn_type spark --conn_host "spark://spark-master" --conn_port "7077"
exec airflow webserver  &> /dev/null &

exec airflow scheduler

