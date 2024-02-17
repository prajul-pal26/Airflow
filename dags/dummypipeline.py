from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime as dtime

def_args={
    "owner": "airflow",
    "start_date": dtime(2022, 1, 1),
    "timezone": "UTC"  # Setting timezone to UTC
}

with DAG('ETL', catchup=False, default_args=def_args) as dag:
    start = DummyOperator(task_id="START")
    E = DummyOperator(task_id="EXTRACTION")
    T = DummyOperator(task_id="TRANSFORMATION")
    L = DummyOperator(task_id="LOADING")
    END = DummyOperator(task_id="END")
    
start >> E >> T >> L >> END 