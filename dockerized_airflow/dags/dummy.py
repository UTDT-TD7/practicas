# Libraries and env variables
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime


default_args = {"depends_on_past": True}

with DAG(
    dag_id="DAG_catedra_airflow_dummy",
    description="Probamos_dummies",
    start_date=datetime(2025, 5, 21, 00, 15),  
    schedule_interval="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    ) as dag:
    
    t1 = DummyOperator(task_id='Dummy_1', dag=dag)
    t2 = DummyOperator(task_id='Dummy_2', dag=dag)
    t3 = DummyOperator(task_id='Dummy_3', dag=dag)
    t4 = DummyOperator(task_id='Dummy_4', dag=dag)
    t5 = DummyOperator(task_id='Dummy_5', dag=dag)
    
    
    # DAG
    t1 >> t2 >>t3
    t3>>[t4,t5]
