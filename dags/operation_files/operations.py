
import pandas as pd


def extract_fn():
    print("Logic to extract data")       #Logic to extract data
    print("print the global variable", GV)         #print the global variable K2 analytics
    rtn_value = "10"
    return rtn_value

def transform_fn(a1, ti):
    xcom_pull_obj = ti.xcom_pull(task_ids="EXTRACTION")  
    extract_rtn_obj = xcom_pull_obj
    print("extract_rtn_obj", extract_rtn_obj)  #extract_rtn_obj 10
    
    print("The value of a1 is", a1)       #The value of a1 is Learning Data Engineering with Airflow
    print("Logic to transform data")      #Logic to transform data
    rtn_value = "20"
    return rtn_value
 
def load_fn(p1, p2, ti):
    x_com_pull_obj = ti.xcom_pull(task_ids="TRANSFORMATION")  
    print(x_com_pull_obj)        # 20
    
    print("The value of p1 is {}".format(p1))              #The value of p1 is k2
    print("The value of p2 is {}".format(p2))              #The value of p2 is analytics
    print("Logic to load data")                             #Logic to load data