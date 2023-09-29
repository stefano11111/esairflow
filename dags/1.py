from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# creating default arg

defdag={"start_date":datetime(2023,9,26),
        "email_on_failure":False,
        "email_on_retry":False,
        "retries":1,
        "retry_delay":timedelta(minutes=5),
        "project_id":1}

with DAG("dag1",schedule_interval=None, default_args=defdag) as dag:
    task0=BashOperator(task_id="bash_task", bash_command="echo 'hello word'")
    task1=BashOperator(task_id="copying_txt", bash_command="cp /opt/airflow/data/DATA_LAKE/dataset_row.txt /opt/airflow/data/DATA_CENTER")
    task2=BashOperator(task_id="removing_txt", bash_command="rm /opt/airflow/data/DATA_LAKE/dataset_row.txt")
    

        #creating dependencies
    task0>>task1>>task2
