from datetime import datetime, timedelta 
from airflow import DAG 
from airflow.operators.python import PythonOperator


defaultargs = {'start_date': datetime(2022, 9, 1),
                    'email_on_failure': False,
                    'email_on_retry': False,
                    'retries': 1,
                    'retry_delay': timedelta(minutes=5),
                    'project_id': 1 }


def python_first_function(): 
    print(datetime.now())

with DAG(dag_id="dag2", schedule_interval=None,default_args=defaultargs) as dag:
    task0=PythonOperator(task_id="printdatetime",python_callable=python_first_function)