import json
from src.etl_framework.GCP.BigQuery import BigQuery
import pendulum
from datetime import datetime
import pandas as pd
import os
from src.etl_framework.utils.utils import read_file_json, convert_epoch_to_datetime
from google.oauth2 import service_account
from src.etl_framework.etl_crypto import ETLcrypto
from src.etl_framework.extract.crawler import Crawler
from airflow.decorators import dag, task

ETL_object = ETLcrypto(api_path="https://www.binance.com/bapi/earn/v1/public/indicator/capital-flow/info?period=MINUTE_15&symbol=BTCUSDT")

@dag(
    schedule_interval='*/4 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

def etl_crypto_analysis_4m():
    @task()
    def extract():
        crawler = Crawler(api_path="https://www.binance.com/bapi/earn/v1/public/indicator/capital-flow/info?period=MINUTE_15&symbol=BTCUSDT")
        return crawler.crawl()

    
    @task
    def load(extract_data: list):
        print("Load to google cloud storage")
        epoch_time = extract_data['updateTimestamp']
        print(epoch_time)
        ETL_object.load(platform="cloud", data=extract_data, data_filetype='.csv', cloud_bucket='raw-crypto-data', crypto_name='bitcoin', extract_param='analysis', extract_length='4m', epoch_time=epoch_time)
        
    # @task
    # def import_to_bq():
    #     config_data = read_file_json('/opt/airflow/dags/src/etl_framework/config/BQ_4m_analysis.json')
    #     service_account_info = read_file_json('/opt/airflow/dags/src/etl_framework/config/cloudace-project-demo-cloud-storage-key.json')
    #     credentials = service_account.Credentials.from_service_account_info(service_account_info)
    #     big_query = BigQuery(config=config_data, credentials=credentials)
    #     if config_data['create_table']:
    #         big_query.create_table()
    #     if config_data['time_T-1']:
    #         big_query.load_from_gcs(bucket_name='raw-crypto-data', folder_name='crypto_data/analysis/4m/bitcoin/', storage_credentials=credentials)
    #     else:
    #         big_query.load_all_from_gcs(bucket_name='raw-crypto-data', folder_name='crypto_data/analysis/4m/bitcoin/', storage_credentials=credentials)
    
    crypto_daily_data= extract()
    load(crypto_daily_data)
    
etl_crypto_analysis_4m()
