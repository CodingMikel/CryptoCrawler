import json

def parse_api_response_to_json(response=None):
    if response:
        return response.json()
        # print(type(result))
        # print(type(json.dumps(result, indent=4)))
        # return json.dumps(result, indent=4)
         
def read_file_json(file_path):
    file = open(file_path)
    data = json.load(file)
    file.close()
    return data