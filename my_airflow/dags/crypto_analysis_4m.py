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
from airflow import AirflowException

ETL_object = ETLcrypto(api_path="https://www.binance.com/bapi/earn/v1/public/indicator/capital-flow/info?period=MINUTE_15&symbol=BTCUSDT")

@dag(
    schedule_interval='*/4 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

def etl_crypto_analysis_4m():
    @task(retries=3, retry_delay=pendulum.duration(minutes=1))
    def extract():
        api_path = "https://www.binance.com/bapi/earn/v1/public/indicator/capital-flow/info?period=MINUTE_15&symbol=BTCUSDT"
        try:
            crawler = Crawler(api_path=api_path)
            data = crawler.crawl()
            if 'updateTimestamp' not in data:
                raise ValueError("API response is missing 'updateTimestamp'")
            return data
        except Exception as e:
            raise AirflowException(f"Error fetching data: {str(e)}")

    
    @task(retries=3, retry_delay=pendulum.duration(minutes=1))
    def load(extract_data: dict):
        try:
            print("Load to google cloud storage")
            epoch_time = extract_data['updateTimestamp']
            print(epoch_time)
            ETL_object.load(platform="cloud", data=extract_data, data_filetype='.csv', cloud_bucket='raw-crypto-data', crypto_name='bitcoin', extract_param='analysis', extract_length='4m', epoch_time=epoch_time)
        except Exception as e:
            raise AirflowException(f"Error loading data to GCS: {str(e)}")

    crypto_daily_data= extract()
    load(crypto_daily_data)
    
etl_crypto_analysis_4m()
