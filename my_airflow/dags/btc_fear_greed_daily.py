import json
from airflow import DAG
from airflow.models import Connection
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago
from airflow.utils.db import provide_session
import os

# Replace with your actual values
GCE_INSTANCE_NAME = 'airflow-demo-project-cloudace'
LOCATION = 'asia-southeast1-c'
SSH_USER = 'minhnguyen'
PROJECT_ID = 'cloudace-project-demo' 
IMPERSONATION_CHAIN = "cloud-storage-sa@cloudace-project-demo.iam.gserviceaccount.com"
credential_path = "/opt/airflow/dags/src/etl_framework/config/cloudace-project-demo-cloud-storage-key.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Define the DAG
dag = DAG(
    'gce_ssh_dag',
    default_args=default_args,
    description='A simple DAG to run a script on a GCE instance',
    schedule_interval=None,
)

# Define the SSH hook
ssh_hook = ComputeEngineSSHHook(
    user=SSH_USER,
    instance_name=GCE_INSTANCE_NAME,
    zone=LOCATION,
    gcp_conn_id='google-gce',
    project_id=PROJECT_ID,
    use_oslogin=False, 
    use_iap_tunnel=False, 
    cmd_timeout=100,
    impersonation_chain=IMPERSONATION_CHAIN,
)

# Task to SSH and run the script on the GCE instance
run_script = SSHOperator(
    task_id='run_script',
    ssh_hook=ssh_hook,
    command="echo hello",
    dag=dag,
)

run_script

print("hello")