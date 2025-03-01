# testing/test_code.py
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from downloader import get_fred_data, get_metadata
from utilities import get_key

# Uncomment to run test for get_key
fred_api_key = get_key()
print(fred_api_key)

tags = ['PERMIT','PERMITNSA']

series_id = 'PERMIT'

# Get Metadata
meta = get_metadata(series_id, fred_api_key)
meta.info()