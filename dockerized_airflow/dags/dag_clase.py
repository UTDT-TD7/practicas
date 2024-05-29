# Libraries and env variables
import os
import random
import sys
from datetime import datetime

import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

import fun.Steps as step

# env_variables
host_1 = "postgres_e"
host_2 = "postgres_l"
port = "5432"
database = "postgres"
# NUNCA hagan esto (hardcodear claves)
user = "catedra"
password = "S3cret"

# Extract query
limit_1 = int(random.random() * 100)

column_names_e = [
    "sessionid",
    "iteminsession",
    "userid",
    "ts",
    "auth",
    "level",
    "trackid",
    "song",
    "artist",
    "zip",
    "city",
    "state",
    "useragent",
    "lon",
    "lat",
    "lastname",
    "firstname",
    "gender",
    "registration",
    "duration",
]

column_names_2 = [
    "sessionid",
    "iteminsession",
    "userid",
    "ts",
    "auth",
    "level",
    "trackid",
    "song",
    "artist",
    "zip",
    "city",
    "state",
    "useragent",
    "lon",
    "lat",
    "lastname",
    "firstname",
    "gender",
    "registration",
    "duration",
    "fecha",
]


extract_query_1 = (
    "SELECT * FROM listen_events OFFSET "
    + str(limit_1)
    + " ROWS FETCH NEXT 10 ROWS ONLY"
)

##TO-DO hay que hacer algo con la lógica aca, un campo de id
extract_query_2 = "SELECT * FROM listen_events ORDER BY fecha DESC LIMIT 10"

# Insert query
names = str(column_names_e).replace("[", "").replace("]", "").replace("'", "")
symbol = (
    str(["%s,"] * 21)
    .replace("[", "")
    .replace("]", "")
    .replace("'", "")
    .replace(",,", ",")
)
symbol = symbol[0 : len(symbol) - 1]
insert_query = (
    "INSERT INTO listen_events (" + names + ", fecha" + ") VALUES " + "(" + symbol + ")"
)

# Para el etl
insert_query_etl = "INSERT INTO etl_dump (sessionid, iteminsession, userid, ts, trackid, zip,len,ban,muestreo ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

default_args = {"depends_on_past": True}


with DAG(
    dag_id="DAG_catedra_airflow",
    description="Clase pasada transformada en DAG",
    start_date=datetime(2024, 5, 28, 00, 15),  ##3 hs adelantado!!!
    schedule_interval="*/5 * * * *",
    default_args=default_args,
    max_active_runs=1,
) as dag:
    t1 = PythonOperator(
        task_id="first-step-el",
        python_callable=step.step_ingesta_carga_pura,
        provide_context=True,
        op_kwargs={
            "host_1": host_1,
            "database": database,
            "user": user,
            "password": password,
            "port": port,
            "extract_query": extract_query_1,
            "column_names_e": column_names_e,
            "host_2": host_2,
            "insert_query": insert_query,
        },
    )

    t2 = PythonOperator(
        task_id="second-step-el",
        python_callable=step.step_ingesta_transformacion_carga,
        provide_context=True,
        op_kwargs={
            "host": host_2,
            "database": database,
            "user": user,
            "password": password,
            "port": port,
            "extract_query": extract_query_2,
            "column_names_e": column_names_2,
            "insert_query": insert_query_etl,
        },
    )
    # DAG
    t1 >> t2
