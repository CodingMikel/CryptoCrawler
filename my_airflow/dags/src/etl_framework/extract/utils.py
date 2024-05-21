import json

def parse_api_response_to_json(response=None):
    if response:
        return response.json()
