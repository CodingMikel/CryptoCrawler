from airflow.utils.dates import days_ago
import json
import jwt
import time
import requests
from airflow.decorators import dag, task
from airflow.utils.log.secrets_masker import mask_secret

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}
@dag(
    default_args=default_args,
    description='All coin analysis',
    schedule_interval='*/4 * * * *',
    start_date=days_ago(1),
    tags=['example']
)
def all_coin_analysis_4m():
    
    @task
    def get_token():
        # Load the service account key file
        key_file_path = '/opt/airflow/dags/src/etl_framework/config/cloudace-project-demo-cloud-storage-key.json'
        with open(key_file_path) as key_file:
            service_account_info = json.load(key_file)

        # Define the JWT headers
        headers = {
            "alg": "RS256",
            "typ": "JWT"
        }

        # Define the JWT payload
        now = int(time.time())

        payload = {
            "iss": service_account_info['client_email'],
            "sub": service_account_info['client_email'],
            "aud": "https://www.googleapis.com/oauth2/v4/token",
            "exp": now + 3600,
            "iat": now,
            "target_audience": "https://asia-southeast1-cloudace-project-demo.cloudfunctions.net/all-coin-analysis-4m"
        }

        # Sign the JWT
        signed_jwt = jwt.encode(payload, service_account_info['private_key'], algorithm='RS256', headers=headers)

        # Prepare the request to exchange JWT for an ID token
        url = 'https://www.googleapis.com/oauth2/v4/token'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        body = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': signed_jwt
        }

        # Make the request
        response = requests.post(url, headers=headers, data=body)
        response_data = response.json()

        # Check for errors in the response
        if response.status_code != 200:
            raise Exception(f"Error exchanging JWT for ID token: {response_data}")
        else:
            id_token = response_data.get('id_token')
            mask_secret(id_token)
            return id_token

    @task
    def invoke_cloud_function(id_token: str):
        url = 'https://asia-southeast1-cloudace-project-demo.cloudfunctions.net/all-coin-analysis-4m'
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {id_token}"}
        data = json.dumps({'message': 'Triggered from Airflow!'})

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            print("Request successful")
        else:
            raise Exception(f"Request failed: {response.status_code}, {response.text}")

    id_token = get_token()
    invoke_cloud_function(id_token)

all_coin_analysis_4m()