from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as dtime, timedelta
from airflow import DAG
import pandas as pd

import os 
root_folder_path="/opt/airflow/dags/operation_files"
os.chdir(root_folder_path)

exec(open('./init.py').read())
exec(open('./xcom_remove_oper.py').read())

def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1), 
    "start_date": dtime(2022, 6, 15),
}


with DAG('remove_xcom', catchup=False, default_args=def_args) as dag:  # Renamed DAG ID
    start = DummyOperator(task_id="START")
    
    e_t_l=PythonOperator(task_id ="EXTRACTION_TRANSFORMATION_LOADING",
                         python_callable=etl,
                         op_args=["Learning Data Engineering with Airflow","K2","Analytics"],
                       do_xcom_push=True 
                       )
    END = DummyOperator(task_id="END")

start >> e_t_l>> END
                                                                                           