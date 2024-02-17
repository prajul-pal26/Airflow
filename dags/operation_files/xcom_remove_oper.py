
import pandas as pd
def etl(a1,p1,p2,ti):
    e_rtn_object=extract_fn()
    l_rtn_object=transform_fn(a1,e_rtn_object)
    l_rtn_object= load_fn(p1,p2,e_rtn_object)
    return None
    


def extract_fn():
    print("Logic to extract data" )       #Logic to extract data
    print("print the global variable", GV)         #print the global variable K2 analytics
    details={
        'cus_id':[1,2,3,4],
        'name':['rejesh','jony','k2','goddy']
    }
    df=pd.DataFrame(details)
    return df

def transform_fn(a1, e_rtn_object):
    extract_rtn_obj=e_rtn_object
    print( extract_rtn_obj)  #extract_rtn_obj 10
    print("The value of a1 is", a1)       #The value of a1 is Learning Data Engineering with Airflow
    print("Logic to transform data")      #Logic to transform data
    rtn_value = "20"
    return rtn_value
 
def load_fn(p1, p2, e_rtn_object):
    extract_rtn_obj=e_rtn_object
 
     
    print(extract_rtn_obj)
    
    print("The value of p1 is {}".format(p1))              #The value of p1 is k2
    print("The value of p2 is {}".format(p2))              #The value of p2 is analytics
    print("Logic to load data")                             #Logic to load data