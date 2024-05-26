FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt clase_utdt ./
COPY profiles.yml /root/.dbt/
RUN pip install --no-cache-dir -r requirements.txt
RUN apt-get update && apt-get install -y vim 




 

