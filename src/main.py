# main.py
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from downloader import get_fred_data
from utilities import get_key

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
        tags = input(f"\nList FRED data tags separated by commas: ").strip().upper().split(",")
    
    elif choice == "2":
        print(f"\nExiting Program... Good bye!")

    all_data = pd.DataFrame()
    invalid_tags = []

    # Get API Key
    fred_api_key = get_key()

    # Get FRED Data
    df = get_fred_data(fred_api_key,tags)
    


    



    # for tag in tags:
        #print(f"\nFetching data for {tag}...")





    

if __name__ == "__main__":
    main()