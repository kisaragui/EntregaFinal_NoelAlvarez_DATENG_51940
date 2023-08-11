FROM  apache/airflow:latest

USER root

WORKDIR $AIRFLOW_HOME

COPY requirements.txt . 
COPY dags .
COPY .env .

USER airflow

RUN pip install -r requirements.txt 
RUN python -m spacy download es_core_news_sm && \
    python -m spacy link es_core_news_sm es 
RUN python -m spacy download en_core_web_sm && \
    python -m spacy link en_core_web_sm eN 