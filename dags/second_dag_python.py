from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

##python functions
def greet(name, age):
    print(f"Hello World! My name is {name} "
          f"and my age is {age}")

default_args = {
    'owner': 'Piyush',
    'retries': 5,
    'retry_delays' : timedelta(minutes=2)
}

with DAG(
    dag_id='python_dag_v02',
    default_args=default_args,
    description='this is a test dag with python operator',
    start_date=datetime(2023, 12, 16),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=greet,
        op_kwargs={'name': 'Piyush', 'age': 33}
    )

    task1
