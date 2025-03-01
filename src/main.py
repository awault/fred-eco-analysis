# main.py
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))


from utilities import get_key
from downloader import get_metadata
from downloader import fetch_fred_series

from downloader import get_fred_data

# ANSI formatting codes
GREEN = '\033[32m'
RED = '\033[31m'
BOLD = '\033[1m'
RESET = '\033[0m'

def main():
    print(f"\n{BOLD}---- FRED DATA ACCESS ----{RESET}\n")
    print(f"Select an option:\n")
    print(f"1) Fetch a list of economic data points.")
    print(f"2) Exit Program")

    choice = input(f"\nEnter your selection: ").strip()
    
    if choice == "1":
        tags = input(f"\nList FRED data series tags separated by commas: ").strip().upper().split(",")
    
    elif choice == "2":
        print(f"\nExiting Program... Good bye!")
        return None

    # Get API Key
    fred_api_key = get_key()

    # Initiate Dataframes
    metadata_df = pd.DataFrame()
    time_series_df = pd.DataFrame()

    # Track invalid tags
    invalid_tags = []

    for tag in tags:
        print(f"\nFetching data for {tag}...")
        
        try: # Get metadata          
            meta = get_metadata(tag,fred_api_key)

            if meta is None or meta.empty:
                print(f'Empty Metadata for tag: {tag}')
                invalid_tags.append(tag)

            else:
                metadata_df = pd.concat([metadata_df, meta], ignore_index=True)
                
        except Exception as e:
            print(f'Error fetching metadata for {tag}: {str(e)}')
            invalid_tags.append(tag)
###
        try:# Get time series          
            series = fetch_fred_series(tag, fred_api_key,start_date='1999-12-31')

            if series is None or series.empty:
                print(f"Series data missing for tag: {tag}")
                invalid_tags.append(tag)

            else:
                time_series_df = pd.concat([time_series_df,series],ignore_index=True)
        
        except Exception as e:
            print(f"Error fetching records for {tag}: {str(e)}")

        print(f"\nInvalid tags: {invalid_tags}\n")
        print(metadata_df.info())
        print("\n")
        print(time_series_df.tail())


if __name__ == "__main__":
    main()