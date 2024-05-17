import requests
from utils.utils import parse_api_response_to_json

class Crawler:
    def __init__(self, api_path=None):
        self.api_path = api_path
        
    def crawl(self):
        response = requests.get(self.api_path)
        response_json = parse_api_response_to_json(response=response)
        return response_json['data']