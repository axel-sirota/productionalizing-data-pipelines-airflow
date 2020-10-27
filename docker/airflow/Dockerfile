# Base Image
FROM bde2020/hadoop-datanode
COPY --from=puckel/docker-airflow / /

ENV SPARK_VERSION=2.4.1
RUN apt-get update -y && apt-get install -y wget \
      && wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-without-hadoop.tgz
RUN tar -xvzf spark-${SPARK_VERSION}-bin-without-hadoop.tgz \
      && mv spark-${SPARK_VERSION}-bin-without-hadoop spark
ENV AIRFLOW_HOME=/usr/local/airflow
WORKDIR $AIRFLOW_HOME
RUN mkdir -p $AIRFLOW_HOME/data
COPY ./start-airflow.sh $AIRFLOW_HOME/start-airflow.sh
RUN chmod +x ./start-airflow.sh
COPY ./airflow.cfg $AIRFLOW_HOME/airflow.cfg
COPY data $AIRFLOW_HOME/data/data.csv
RUN chown -R airflow:airflow $AIRFLOW_HOME
ENV PATH="/spark/bin:${PATH}"
RUN bash -l -c 'echo export SPARK_DIST_CLASSPATH=$(hadoop classpath) >> /etc/bash.bashrc'
CMD [ "./start-airflow.sh" ]
