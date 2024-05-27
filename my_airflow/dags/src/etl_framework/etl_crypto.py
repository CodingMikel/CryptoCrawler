from .etl_base import ETLpipeline
import pandas as pd
import os
from .utils.utils import read_file_json, convert_epoch_to_datetime
from google.oauth2 import service_account
from google.cloud import storage
from io import BytesIO

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
    

class ETLcrypto(ETLpipeline):
    def load(self, platform=None, data=None, data_filetype=None, cloud_bucket=None, crypto_name=None, extract_param=None, extract_length=None, epoch_time=None):
        if platform == "cloud" and data is not None and epoch_time is not None:
            value = pd.DataFrame([data])
            print(value.head())
            date_time = convert_epoch_to_datetime(epoch_time)
            datetime_array =str(date_time).split()
            date_array = datetime_array[0].split('-')
            time_array = datetime_array[1].split(':')
            cloud_filepath = "crypto_data/{}/{}/{}/year={}/month={}/day={}/hour={}/minute={}/".format(extract_param, extract_length, crypto_name, date_array[0], date_array[1], date_array[2], time_array[0], time_array[1])
            print(cloud_filepath)
            file_path = cloud_filepath + extract_param + "_" + extract_length + ".csv"
            
            buffer = BytesIO()
            with pd.io.common.get_handle(buffer, mode='w', encoding=None) as handle:
                value.to_csv(handle.handle, index=False)
            buffer.seek(0)
            upload_to_gcs(cloud_bucket, file_path, buffer)