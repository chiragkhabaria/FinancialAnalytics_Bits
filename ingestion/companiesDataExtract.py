import sys
import os

path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
print(path)
sys.path.insert(1,path)


import json 
import certifi 
import requests
import configuration.sconfig
#import sconfig


def ingestDJIA():
    try:
        secretAPI = configuration.sconfig.secretsConfig()
        api_url = f'https://financialmodelingprep.com/api/v3/dowjones_constituent?apikey={secretAPI.Tokens.financialmodelingprep}'
        response = requests.get(api_url)
        json_data = response.json()
        
        with open(secretAPI.DataLocations.djiacompanies, 'w') as f:
            json.dump(json_data, f)
        
    except Exception as e:
        raise Exception(f'Error occurred while fetching Dow Jones Index Companies list. Error Details : {str(e)}')
