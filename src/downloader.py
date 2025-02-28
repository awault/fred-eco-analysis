# src/downloader.py

from dotenv import load_dotenv
from fredapi import Fred
import os
import pandas as pd
import requests

def get_metadata(series_id, api_key):
    """
    Fetch metadata for a given FRED series ID.

    Args:
        series_id (str): A FRED series ID to retrieve metadata
        api_key (str): The FRED API key for authentication

    Returns:
        pd.DataFrame: A DataFrame containing the metadata for the series.
    """
    # Construct the API request URL
    url = f'https://api.stlouisfed.org/fred/series?series_id={series_id}&api_key={api_key}&file_type=json'

    # Send the request to FRED API
    response = requests.get(url)

    # Check if the response was successful
    if response.status_code != 200:
        print(f"Error fetching metadata. HTTP Status Code: {response.status_code}")
        return None
    
    # Parse the JSON response
    data = response.json()

    # Extract metadata if it exists
    if "seriess" not in data or not data["seriess"]:
        raise ValueError(f"Unexpected API response or invalid series ID: {series_id}")
        
    data = pd.DataFrame([data["seriess"][0]])
    
    return data




def get_fred_data(fred_api_key,series_id, start_date=None, end_date=None):
    """
    Fetches time series data from FRED using tags or series id.

    Parameters:
        series_id (str): FRED series id or tag (e.g. 'GDP','PERMIT')
        start_date (str, optional): Start date in 'YYYY-MM-DD' format.
        end_date (str, optional): End date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: A DataFrame with the date as index and values as the series data.
    """
    
    fred = Fred(api_key=str(fred_api_key))
    
    try:
        data = fred.get_series(series_id,start_date,end_date)
        df = pd.DataFrame(data, columns=['Value'])
        df.index.name = 'Date'
        return df

    except Exception as e:
        print(f"Error fetching data: {e}")
        return None