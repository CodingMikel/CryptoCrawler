import json

def parse_api_response_to_json(response=None):
    if response:
        return response.json()
         
def read_file_json(file_path):
    file = open(file_path)
    data = json.load(file)
    file.close()
    return data