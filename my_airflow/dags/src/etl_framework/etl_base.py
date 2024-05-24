from .extract.crawler import Crawler
from .utils.utils import read_file_json
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.oauth2 import service_account
from datetime import datetime
import pandas as pd
import os
from google.cloud import storage
from io import BytesIO
import json

# from google.cloud import storage_v1
def upload_to_gcs(bucket_name, destination_blob_name, data):
    service_account_info = read_file_json('/opt/airflow/dags/src/etl_framework/config/cloudace-project-demo-cloud-storage-key.json')
    if service_account_info:
        credentials = service_account.Credentials.from_service_account_info(
        service_account_info)
        storage_client = storage.Client(credentials=credentials)
    else:
        storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(data, content_type='text/csv')
    

class ETLpipeline:
    def __init__(self, config_path=None, api_path=None):
        self.config_path = config_path
        self.config = read_file_json(config_path) if config_path is not None else None
        self.api_path=api_path
    
    def extract(self, crypto_name=None, extract_param=None, extract_length=None):
        if extract_param == "price" and extract_length == "24h":
            coin_price_24h_crawler = Crawler(api_path=self.api_path + crypto_name + "?type=" + extract_length)
            coin_price_24h_data = coin_price_24h_crawler.crawl()
            
            price_store_by_date = {}
            for item in coin_price_24h_data:
                key = datetime.fromtimestamp(item[0]).date().strftime("%Y-%m-%d")
                value = {'date': key, 'extract_time': datetime.fromtimestamp(item[0]).time().strftime("%H:%M"), 'price': item[1]}
                if key not in price_store_by_date:
                    price_store_by_date[key] = [value]
                price_store_by_date[key].append(value)
            return price_store_by_date
    
    def load(self, platform=None, data=None, data_filetype=None, cloud_bucket=None, crypto_name=None, extract_param=None, extract_length=None):
        if platform == "local" and data is not None and extract_param == "price" and extract_length == "24h":
            for date in data:
                value = pd.DataFrame(data[date])
                date = date.split("-")
                local_filepath = "coin_data/{}/{}/{}/{}/".format(crypto_name, date[0], date[1], date[2])
                
                # Create the directory if it does not exist
                os.makedirs(local_filepath, exist_ok=True)
                file_path = local_filepath + 'price_daily' + data_filetype
                if data_filetype == '.csv.gz':
                    value.to_csv(file_path, index=False, compression='gzip')
                elif data_filetype == '.csv':
                    value.to_csv(file_path, index=False)
        elif platform == "cloud" and data is not None and extract_param == "price" and extract_length == "24h":
            for date in data:
                value = pd.DataFrame(data[date])
                date = date.split("-")
                cloud_filepath = "crypto_data/{}/{}/{}/{}/{}/{}/".format(extract_param, extract_length,crypto_name, date[0], date[1], date[2])
                file_path = cloud_filepath + extract_param + "_" + extract_length + ".csv"
                buffer = BytesIO()
                with pd.io.common.get_handle(buffer, mode='w', encoding=None) as handle:
                    value.to_csv(handle.handle, index=False)
                buffer.seek(0)
                
                upload_to_gcs(cloud_bucket, file_path, buffer)
            