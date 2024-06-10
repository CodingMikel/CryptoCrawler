import requests
import json
from google.cloud import bigquery
import time

def insert_into_bigquery(data):
    """Inserts API data into a BigQuery table.

    Args:
        data (dict): The API record to insert.
    """

    # Replace with your BigQuery project ID, dataset ID, and table ID
    project_id = 'cloudace-project-demo' 
    dataset_id = 'crypto_data'
    table_id = 'btc_price_1s'

    # Construct the full BigQuery table ID
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Create a BigQuery client
    client = bigquery.Client()

    # Prepare data as a list of rows (for batch insertion - more efficient)
    rows_to_insert = [data]  # Since you have a single record

    # Insert data into BigQuery
    errors = client.insert_rows_json(table_ref, rows_to_insert) 

    if errors == []:
        print("Data inserted into BigQuery successfully.")
    else:
        print(f"Encountered errors while inserting rows: {errors}")

btc_price_1s_api = "https://www.binance.com/api/v3/uiKlines?limit=1&symbol=BTCUSDT&interval=1s"

start_time = time.time()
while time.time() - start_time < 58:  # Run for about 1 minute 
    x = requests.get(btc_price_1s_api)

    try:
        x.raise_for_status() 
        data = x.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        time.sleep(1) # Wait 1 second before retrying
        continue  # Skip to the next iteration of the loop

    data_array = data[0]
    result = {
        'time_started': data_array[0] / 1000,
        'open': data_array[1],
        'high': data_array[2],
        'low': data_array[3],
        'close': data_array[4],
        'time_end': (data_array[6] + 1) / 1000 
    }

    print(result)
    insert_into_bigquery(result)

    time.sleep(1)  # Wait for 1 second before the next iteration