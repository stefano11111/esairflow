import time 
import json 
from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago


default_args = { 'start_date': datetime(2022,9,1),
                'owner': 'airflow',
                'retries': 1, 'retry_delay': timedelta(minutes=5)}

drop="""DROP TABLE IF EXISTS person"""

createtable="""CREATE TABLE person (name VARCHAR NOT NULL, age INT)"""

insert_data="""INSERT INTO person(name,age) VALUES ('ste',28),('alex',25),('lucia',35)"""

calculating_averag_age = """SELECT avg(age) FROM person"""

with DAG("dagsql1", schedule_interval=None,catchup=False,default_args=default_args) as dag:
    task0=PostgresOperator(task_id="drop_table",sql=drop,postgres_conn_id="ste")
    task1=PostgresOperator(task_id="create_table",sql=createtable,postgres_conn_id="ste")
    task2=PostgresOperator(task_id="insert",sql=insert_data,postgres_conn_id="ste")
    task3=PostgresOperator(task_id="avg_age",sql=calculating_averag_age,postgres_conn_id="ste")
    task0>>task1>>task2>>task3