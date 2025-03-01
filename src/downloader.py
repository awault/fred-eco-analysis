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



def fetch_fred_series(series_id,api_key,start_date=None,end_date=None):
    """
    Fetches time series data from the FRED API and returns it as a pandas DataFrame.
    
    Parameters:
    series_id (str): The ID of the FRED series to fetch.
    api_key (str): API key for authenticating with the FRED API.
    start_date (str, optional): Start date for the data retrieval in 'YYYY-MM-DD' format.
    end_date (str, optional): End date for the data retrieval in 'YYYY-MM-DD' format.
    
    Returns:
    pd.DataFrame or None: A DataFrame containing the time series data if successful, otherwise None.
    """
    # Construct the base API URL with required parameters
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&\
        api_key={api_key}&file_type=json'
    
    # Append optional start and end dates to the URL if provided
    if start_date:
        url += f'&observation_start={start_date}'

    if end_date:
        url += f'&observation_end={end_date}'

    # Make the GET request to the FRED API
    response = requests.get(url)

    # Check if the response status is not 200 (OK)
    if response.status_code != 200:
        print(f"Error fetching data. HTTP Status Code: {response.status_code}")
        return None
    
    try:
        # Try to load the response JSON
        data = response.json()

        # Check if observations key exists and contains data, if not return None
        if "observations" not in data or not data["observations"]:
            print(f"No observations found for the series: {series_id}")
            return None
        
        else: # Convert the observations into a DataFrame
            df = pd.DataFrame(data['observations'])
            df["id"] = series_id
            return df
        
    except ValueError as e:
        # Handle JSON parsing errors
        print(f"Error parsing the response JSON for series: {series_id}. Error: {str(e)}")
        return None
