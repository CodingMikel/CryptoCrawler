import requests
from .utils import parse_api_response_to_json

class Crawler:
    def __init__(self, api_path=None):
        self.api_path = api_path
        
    def crawl(self):
        response = requests.get(self.api_path)
        response_json = parse_api_response_to_json(response=response)
        if 'data' in response_json:
            return response_json['data']
        else:
            return response_json