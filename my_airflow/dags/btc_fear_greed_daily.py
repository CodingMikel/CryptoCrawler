from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import json
from dotenv import load_dotenv
import os

load_dotenv()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

auth_token = os.getenv("AUTH_TOKEN")
auth_token = str(auth_token)

with DAG(
    'trigger_with_http_operator',
    default_args=default_args,
    description='A simple HTTP operator example DAG',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    trigger_task = SimpleHttpOperator(
        task_id='invoke_cloud_function',
        http_conn_id='http_default',
        method='GET',
        endpoint='/test-1',
        data=json.dumps({'message': 'Triggered from Airflow!'}),
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + auth_token},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
    )

    trigger_task