from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import json


dag = DAG(
        'kharisma-task2-airflow',
        description='Hello World, Task 2 DAG Kharisma!',
        schedule_interval='0 */5 * * *',
        start_date=datetime(2020, 8, 23),
        catchup=False
)
predict_names_task = SimpleHttpOperator(
    task_id='profile_from_gender',
    method='POST',
    http_conn_id='gender_api',
    endpoint='/gender/by-first-name-multiple',
    headers={"Content-Type": "application/json"},
    data=json.dumps([
  {
    "first_name": "sandra",
    "country": "US"
  },
  {
    "first_name": "mike",
    "country": "CA"
  }
]),
    response_filter=lambda response: json.loads(response.text),
    log_response=True,
    dag=dag,
)
create_table_task = PostgresOperator(
    task_id='create_table_to_postgres',
    postgres_conn_id='pg_conn_id',
    sql="""
    CREATE TABLE IF NOT EXISTS kharisma_gender_name_prediction (
        input JSONB,
        details JSONB,
        result_found BOOLEAN,
        first_name VARCHAR(50),
        probability FLOAT,
        gender VARCHAR(10),
        timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)
def load_predictions_to_postgres(**kwargs):
    ti = kwargs['ti']
    predictions = ti.xcom_pull(task_ids='profile_from_gender')
    pg_hook = PostgresHook(postgres_conn_id='pg_conn_kharisma')
    for prediction in predictions:
        input_data = json.dumps(prediction['input'])
        details_data = json.dumps(prediction['details'])
        result_found = prediction['result_found']
        first_name = prediction['first_name']
        probability = prediction['probability']
        gender = prediction['gender']
        pg_hook.run("""
            INSERT INTO kharisma_gender_name_prediction (input, details, result_found, first_name, probability, gender)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, parameters=(input_data, details_data, result_found, first_name, probability, gender))
load_predictions_task = PythonOperator(
    task_id='profile_gender_to_postgres',
    python_callable=load_predictions_to_postgres,
    provide_context=True,
    dag=dag,
)
predict_names_task >> create_table_task >> load_predictions_task