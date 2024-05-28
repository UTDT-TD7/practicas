FROM python:3.9-bookworm

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.1
WORKDIR /opt/airflow 
RUN pip install "apache-airflow==2.9.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.9.txt"
COPY requirements.txt /opt/airflow/ 
RUN pip install -r /opt/airflow/requirements.txt

RUN airflow db migrate && airflow users create --username airflow --firstname Peter --lastname Parker --role Admin --password airflow --email spiderman@superhero.org 

CMD airflow webserver --port 8080 & airflow scheduler
