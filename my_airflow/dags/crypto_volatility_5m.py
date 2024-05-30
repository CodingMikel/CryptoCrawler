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

ETL_object = ETLcrypto(api_path="https://www.binance.com/fapi/v1/marketKlines?symbol=iBTCBVOLUSDT&interval=5m&limit=1")

@dag(
    schedule_interval='*/5 * * * *',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)

def etl_crypto_volatility_5m():
    @task()
    def extract():
        crawler = Crawler(api_path="https://www.binance.com/fapi/v1/marketKlines?symbol=iBTCBVOLUSDT&interval=5m&limit=1")
        return crawler.crawl()
    @task()
    def transform(extract_data: list):
        print("Transform data")
        transformed_data = []
        for item in extract_data:
            dict_item = {}
            dict_item['time_start'] = item[0]
            dict_item['open'] = item[1]
            dict_item['high'] = item[2]
            dict_item['low'] = item[3]
            dict_item['close'] = item[4]
            dict_item['time_end'] = item[6]
            transformed_data.append(dict_item)
            dict_item = {}
        return transformed_data
    
    @task
    def load(transformed_data: list):
        print("Load to google cloud storage")
        ETL_object.load(platform="cloud", data=transformed_data, data_filetype='.csv', cloud_bucket='raw-crypto-data', crypto_name='bitcoin', extract_param='volatility', extract_length='5m')
    crypto_extract_data = extract()
    crypto_transform_data = transform(crypto_extract_data)
    load(crypto_transform_data)
    
etl_crypto_volatility_5m()
