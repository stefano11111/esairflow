import requests 
import time 
import json 
from airflow import DAG 
from airflow.operators.python import PythonOperator 
from airflow.operators.python import BranchPythonOperator 
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
import pandas as pd 
import numpy as np 
import os


defaultargs = {'start_date': datetime(2022, 9, 1),
                    'email_on_failure': False,
                    'email_on_retry': False,
                    'retries': 1,
                    'retry_delay': timedelta(minutes=5),
                    'project_id': 1 }

def get_data(tickers,**kwargs): 
    l=tickers
    for x in l:
        api="JC7HIING3L8QAD4X"
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={x}&apikey="+api
        db=requests.get(url).json()
        with open(f"/opt/airflow/data/DATA_LAKE/data{x}.json","w") as outfile:
            json.dump(db,outfile)
    


with DAG("market_data_alphavantage_dag", schedule_interval = '@daily', catchup=False, default_args = defaultargs) as dag_python:
    task_0 = PythonOperator(task_id = "get_market_data", python_callable = get_data, op_kwargs = {'tickers' : ["META","AAPL"]})