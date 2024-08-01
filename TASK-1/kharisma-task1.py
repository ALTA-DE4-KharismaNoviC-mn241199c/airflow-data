from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

def print_hello():
    return 'Hello world from first Airflow DAG!'

dag = DAG(
        'kharisma-task1-airflow',
        description='Hello World, Task 1 DAG Kharisma!',
        schedule_interval='0 */5 * * *',
        start_date=datetime(2020, 8, 23),
        catchup=False
)

start = EmptyOperator(
    task_id='start',
    dag=dag,
)

def push_xcom(**kwargs):
    value = 'nilai_saya'
    kwargs['ti'].xcom_push(key='kunci_saya', value=value)

push_task = PythonOperator(
    task_id='push_task',
    provide_context=True,
    python_callable=push_xcom,
    dag=dag,
)

def pull_xcoms(**kwargs):
    ti = kwargs['ti']
    value1 = ti.xcom_pull(task_ids='push_task', key='kunci_saya')
    value2 = ti.xcom_pull(task_ids='task_lainnya', key='kunci_lainnya')
    print(f'Nilai yang ditarik: {value1}, {value2}')

pull_task = PythonOperator(
    task_id='pull_task',
    provide_context=True,
    python_callable=pull_xcoms,
    dag=dag,
)

start >> push_task >> pull_task