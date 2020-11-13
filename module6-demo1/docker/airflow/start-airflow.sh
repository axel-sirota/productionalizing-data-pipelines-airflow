#!/usr/bin/env bash

# Move to the AIRFLOW HOME directory
cd $AIRFLOW_HOME

# Initiliase the metadatabase
airflow initdb
# shellcheck disable=SC2016
exec airflow webserver  &> /dev/null &

exec airflow scheduler

