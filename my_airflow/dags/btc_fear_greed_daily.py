import json
from src.etl_framework.GCP.BigQuery import BigQuery
import pendulum
from datetime import datetime
import pandas as pd
import os
from src.etl_framework.utils.utils import read_file_json
from google.oauth2 import service_account
from src.etl_framework.etl_base import ETLpipeline
from airflow.decorators import dag, task


ETL_object = ETLpipeline(api_path="https://api.coin-stats.com/v2/fear-greed")

@dag(
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

def etl_btc_fear_greed():
    @task.bash
    def extract():
        return f"gcloud compute ssh --zone=asia-southeast1-c airflow-demo-project-cloudace --command \"python3 /opt/airflow/pipeline_script/btc_fear_greed_daily_script.py\""
    
    extract()
etl_btc_fear_greed()