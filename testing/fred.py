# testing/fred.py

import os 
import pandas as pd
import requests
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from utilities import get_key

# Uncomment to run test for get_key
fred_api_key = get_key()
print(fred_api_key)

tags = ['PERMIT','PERMITNSA']

def fetch_fred_data(series_id,api_key,start_date=None,end_date=None):
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&\
        api_key={api_key}&file_type=json'
    
    if start_date:
        url += f'&observation_start={start_date}'

    if end_date:
        url += f'&observation_end={end_date}'

    response = requests.get(url)

    if response.status_code != 200:
        print(f"Error fetching data. HTTP Status Code: {response.status_code}")
        return None
    
    data = response.json()

    print(data)

#test = fetch_fred_data('PERMIT',fred_api_key,start_date='2024-01-01',end_date='2024-12-31')




# Test metadata retrieval
metadata = fetch_fred_metadata('PERMIT', fred_api_key)
print(metadata)

    