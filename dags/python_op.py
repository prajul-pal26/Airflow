from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime as dtime, timedelta
from airflow import DAG


def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1), 
    "start_date": dtime(2022, 6, 15),
}

def extract_fn():
    print("Logic to extract data")

def transform_fn(a1):
    print("The value of a1 is", a1)
    print("Logic to transform data")

def load_fn(p1, p2):
    print("The value of p1 is {}".format(p1))
    print("The value of p2 is {}".format(p2))
    print("Logic to load data")

with DAG('ex-py-operator', catchup=False, default_args=def_args) as dag:
    start = DummyOperator(task_id="START")
    E = PythonOperator(task_id="EXTRACTION", python_callable=extract_fn)
    T = PythonOperator(task_id="TRANSFORMATION", python_callable=transform_fn, op_args=["Learning Data Engineering with Airflow"])
    L = PythonOperator(task_id="LOADING", python_callable=load_fn, op_args=["K2", "Analytics"])
    END = DummyOperator(task_id="END")

start >> E >> T >> L >> END
