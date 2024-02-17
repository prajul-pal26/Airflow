from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as dtime, timedelta
from airflow import DAG
import pandas as pd

import os 
root_folder_path="/opt/airflow/dags/operation_files"
os.chdir(root_folder_path)

exec(open('./init.py').read())
exec(open('./operations.py').read())

def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1), 
    "start_date": dtime(2022, 6, 15),
}


with DAG('xcom_transfer_data', catchup=False, default_args=def_args) as dag:  # Renamed DAG ID
    start = DummyOperator(task_id="START")
    E = PythonOperator(task_id="EXTRACTION", 
                       python_callable=extract_fn,
                       do_xcom_push=True
                    )
    T = PythonOperator(task_id="TRANSFORMATION",
                       python_callable=transform_fn, 
                       op_args=["Learning Data Engineering with Airflow"],
                       do_xcom_push=True
                    )
    L = PythonOperator(task_id="LOADING",
                       python_callable=load_fn, 
                       op_args=["K2", "Analytics"],
                      
                    )
    END = DummyOperator(task_id="END")

start >> E >> T >> L >> END
                                                                                           