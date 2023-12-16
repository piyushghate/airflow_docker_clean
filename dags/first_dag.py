from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args={
    'owners' : 'Piyush',
    'retries0' : 5,
    'retry_delays' : timedelta(minutes=2)
}

with DAG(
    dag_id='fisrt_dag_v3',
    default_args=default_args,
    description='This is our first airflow dag in this repo',
    start_date=datetime(2023, 12, 16),
    schedule_interval='@daily'
) as dag:
    task1 = BashOperator(
        task_id='first_task',
        bash_command="echo Hello World!, this is the first task!"
    )

    task2 = BashOperator(
        task_id='second_task',
        bash_command="echo This is the second task"
    )

    task3 = BashOperator(
        task_id='third_task',
        bash_command="echo This is third task"
    )

    task1 >> [task2, task3]