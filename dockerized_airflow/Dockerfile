FROM python:3.9-bookworm

ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW_VERSION=2.9.1
WORKDIR /opt/airflow 
RUN pip install "apache-airflow==2.9.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.9.txt"
COPY requirements.txt /opt/airflow/ 
RUN pip install -r /opt/airflow/requirements.txt
RUN echo admin > /opt/airflow/standalone_admin_password.txt
CMD airflow standalone