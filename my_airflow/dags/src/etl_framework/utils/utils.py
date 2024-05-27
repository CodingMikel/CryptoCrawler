import json
from datetime import datetime, timezone

def parse_api_response_to_json(response=None):
    if response:
        return response.json()
         
def read_file_json(file_path):
    file = open(file_path)
    data = json.load(file)
    file.close()
    return data

def convert_epoch_to_datetime(epoch_time):
    # Convert milliseconds to seconds
    epoch_time_seconds = epoch_time / 1000
    return datetime.fromtimestamp(epoch_time_seconds, tz=timezone.utc)