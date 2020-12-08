# Base Image
FROM puckel/docker-airflow

ENV AIRFLOW_HOME=/usr/local/airflow
WORKDIR $AIRFLOW_HOME
RUN mkdir -p $AIRFLOW_HOME/data
RUN mkdir -p $AIRFLOW_HOME/plugins
COPY mongo_plugin $AIRFLOW_HOME/plugins
COPY start-airflow.sh $AIRFLOW_HOME/start-airflow.sh
COPY airflow.cfg $AIRFLOW_HOME/airflow.cfg
COPY data.csv $AIRFLOW_HOME/data/data.csv
COPY sql $AIRFLOW_HOME
USER root
RUN pip install bson pymongo
RUN chmod +x $AIRFLOW_HOME/start-airflow.sh
RUN chown -R airflow:airflow $AIRFLOW_HOME
USER airflow
CMD [ "./start-airflow.sh" ]
