# src/utilities.py

from dotenv import load_dotenv
from fredapi import Fred
import os

env_path = os.path.join(os.path.dirname(__file__), '../.env')
load_dotenv(dotenv_path=env_path)

# Get Key
def get_key():
    fred_api_key = os.getenv('FRED_API_KEY')
    if not fred_api_key:
        print(f"\nAPI key is unavailable!")
    else:
        print(f"\nAPI key loaded successfully!")
    return fred_api_key