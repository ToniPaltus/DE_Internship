from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_data(**context):
    data = "Привет, мир!"
    context['task_instance'].xcom_push(key='data_key', value=data)

def process_data(**context):
    data = context['task_instance'].xcom_pull(task_ids='get_data_task', key='data_key')
    processed_data = data.upper()
    print(processed_data)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'xcom_example_dag',
    default_args=default_args,
    description='Пример DAG с использованием xCom',
    schedule_interval=None
)

get_data_task = PythonOperator(
    task_id='get_data_task',
    python_callable=get_data,
    provide_context=True,
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    provide_context=True,
    dag=dag
)

get_data_task >> process_data_task
