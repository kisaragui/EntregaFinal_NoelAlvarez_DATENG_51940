FROM  apache/airflow:latest

USER root

WORKDIR $AIRFLOW_HOME

COPY requirements.txt . 
COPY dags .
COPY .env .

USER airflow

RUN pip install -r requirements.txt 
