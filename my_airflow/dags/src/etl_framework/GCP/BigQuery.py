from google.cloud import bigquery, storage
from google.oauth2 import service_account
import google.api_core.exceptions
import datetime
# table_id = "your-project.your_dataset.your_table_name"


class BigQuery:
    def __init__(self, config=None, credentials=None):
        self.config=config
        self.columns=config['columns']
        self.project_name=config['project_name']
        self.dataset_name=config['dataset_name']
        self.table_name=config['table_name']
        self.table_id="{}.{}.{}".format(self.project_name, self.dataset_name, self.table_name)
        self.client = bigquery.Client(credentials=credentials) if credentials else bigquery.Client()
        if config['create_table'] is False:
            self.schema = self._get_table_schema()
    
    def _get_table_schema(self):
        try:
            table = self.client.get_table(self.table_id)
            return table.schema
        except Exception as e:
            print(f"An error occurred while fetching the table schema: {e}")
            return None

    def _ensure_dataset_exists(self):
        dataset_id = "{}.{}".format(self.project_name, self.dataset_name)
        try:
            self.client.get_dataset(dataset_id)
            print(f"Dataset {self.dataset_name} already exists.")
        except google.api_core.exceptions.NotFound:
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "asia-southeast1"
            self.client.create_dataset(dataset)
            print(f"Created dataset {self.dataset_name}.")

    def create_table(self):
        self._ensure_dataset_exists()
        
        schema = [
            bigquery.SchemaField(column['name'], column['type'], mode="REQUIRED")
            for column in self.columns
        ]
        
        table_id = "{}.{}.{}".format(self.project_name, self.dataset_name, self.table_name)
        table = bigquery.Table(table_id, schema=schema)
        table = self.client.create_table(table)
        print(
            "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
        )
        self.schema = schema
    
    def load_all_from_gcs(self, bucket_name=None, folder_name=None, storage_credentials=None):
        if storage_credentials:
            storage_client = storage.Client(credentials=storage_credentials)
        else:
            storage_client = storage.Client()
        
        table_ref = self.client.dataset(self.dataset_name).table(self.table_name)
        
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_name)
        for blob in blobs:
            if not blob.name.endswith('/'):  # Skip directories
                gcs_uri = f'gs://{bucket_name}/{blob.name}'
                print(f'Processing file: {gcs_uri}')
                
                # Load data from GCS to BigQuery
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,  # Set the source format to CSV
                    skip_leading_rows=1,  # Skip the header row if your CSV has a header
                    autodetect=True,  # Let BigQuery auto-detect the schema
                    schema=self.schema,  # Provide schema explicitly if not using autodetect
                )

                load_job = self.client.load_table_from_uri(
                    gcs_uri, table_ref, job_config=job_config
                )

                # Wait for the load job to complete
                load_job.result()
                print(f'Loaded {load_job.output_rows} rows from {gcs_uri} into table.')

        print('All files have been processed.')
                
    def load_from_gcs(self, bucket_name=None, folder_name=None, storage_credentials=None):
        if storage_credentials:
            storage_client = storage.Client(credentials=storage_credentials)
        else:
            storage_client = storage.Client()
        
        table_ref = self.client.dataset(self.dataset_name).table(self.table_name)
        
        bucket = storage_client.bucket(bucket_name)
        
        # Get yesterday's date
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        folder_date = yesterday.strftime("year=%Y/month=%m/day=%d")
        print(folder_date)
        folder_path = f"{folder_name}{folder_date}"
        print(folder_path)
        blobs = bucket.list_blobs(prefix=folder_path)
        for blob in blobs:
            if not blob.name.endswith('/'):  # Skip directories
                gcs_uri = f'gs://{bucket_name}/{blob.name}'
                print(f'Processing file: {gcs_uri}')
                # Load data from GCS to BigQuery
                job_config = bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,  # Set the source format to CSV
                    skip_leading_rows=1,  # Skip the header row if your CSV has a header
                    autodetect=True,  # Let BigQuery auto-detect the schema
                    schema=self.schema,  # Provide schema explicitly if not using autodetect
                )

                load_job = self.client.load_table_from_uri(
                    gcs_uri, table_ref, job_config=job_config
                )

                # Wait for the load job to complete
                load_job.result()
                print(f'Loaded {load_job.output_rows} rows from {gcs_uri} into table.')

        print('All files from T-1 date have been processed.')
# minminfighting ilove myself heheheh i dont love min in baibai