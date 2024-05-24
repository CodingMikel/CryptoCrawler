import json

import pendulum
from datetime import datetime
import pandas as pd
import os

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
        
    crypto_daily_data= extract()
    load(crypto_daily_data)
    
etl_crypto_daily_price()