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

ETL_object = ETLpipeline(api_path="https://api.coin-stats.com/v2/coin_chart/")

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
        
    @task
    def import_to_bq():
        config_data = read_file_json('/opt/airflow/dags/src/etl_framework/config/BQ_24h_price.json')
        service_account_info = read_file_json('/opt/airflow/dags/src/etl_framework/config/cloudace-project-demo-cloud-storage-key.json')
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        big_query = BigQuery(config=config_data, credentials=credentials)
        if config_data['create_table']:
            big_query.create_table()
        if config_data['time_T-1']:
            big_query.load_from_gcs(bucket_name='raw-crypto-data', folder_name='crypto_data/price/24h/bitcoin/', storage_credentials=credentials)
        else:
            big_query.load_all_from_gcs(bucket_name='raw-crypto-data', folder_name='crypto_data/price/24h/bitcoin/', storage_credentials=credentials)
    
    crypto_daily_data= extract()
    load(crypto_daily_data) >> import_to_bq()
    
etl_crypto_daily_price()