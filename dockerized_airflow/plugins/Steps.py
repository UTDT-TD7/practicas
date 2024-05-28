from datetime import datetime

import pandas as pd
import psycopg2

import functions


def step_ingesta_carga_pura(
    host_1,
    host_2,
    database,
    user,
    password,
    port,
    extract_query,
    insert_query,
    column_names_e,
):
    # Ingesta
    df = functions.SQL_query(
        host_1, database, user, password, port, extract_query, column_names_e
    )
    df["fecha"] = str(datetime.now())
    # Carga
    functions.transcripcion_completa(
        host_2, database, user, password, port, insert_query, df
    )


def step_ingesta_transformacion_carga(
    host, database, user, password, port, extract_query, insert_query, column_names_e
):
    # Ingesta
    df = functions.SQL_query(
        host, database, user, password, port, extract_query, column_names_e
    )
    # Transform
    df_transf = functions.transf(df)
    # Carga
    functions.transcripcion_completa(
        host, database, user, password, port, insert_query, df_transf
    )
