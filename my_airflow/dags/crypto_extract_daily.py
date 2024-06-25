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
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

ETL_object = ETLpipeline(api_path="https://api.coin-stats.com/v2/coin_chart/")
DAG_ID = "bigquery_check_table_existence"
PROJECT_ID = "cloudace-project-demo"
DATASET_ID = "crypto_data"
TABLE_ID = "btc_price_24h_day_boxplot"

TABLE_FULL_NAME = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
query = f"SELECT * FROM `{TABLE_FULL_NAME}` LIMIT 1"

@dag(
    schedule_interval='0 0 * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

def etl_crypto_daily_price():
    @task()
    def extract():
        return ETL_object.extract(crypto_name="bitcoin", extract_param="price", extract_length="24h")
    
    @task
    def load(extract_data: list):
        print("Load to google cloud storage")
        ETL_object.load(platform="cloud", data=extract_data, data_filetype='.csv', cloud_bucket='raw-crypto-data', crypto_name='bitcoin', extract_param='price', extract_length='24h')

    call_insert_procedure = BigQueryInsertJobOperator(
        task_id="call_insert_procedure",
        configuration={
            "query": {
                "query": "CALL `cloudace-project-demo.crypto_data.insert_btc_price_24h_day_boxplot_daily`(); ",
                "useLegacySql": False,
            }
        },
        location='asia-southeast1',
    )
    
    crypto_daily_data = extract()
    load(crypto_daily_data) >> call_insert_procedure
    
etl_crypto_daily_price()