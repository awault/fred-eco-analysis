# main.py
import sys
import os
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from utilities import get_key
from downloader import get_metadata, fetch_fred_series
from storage import create_table
from sqlalchemy import create_engine, text, types

# ANSI formatting codes
GREEN = '\033[32m'
RED = '\033[31m'
BOLD = '\033[1m'
RESET = '\033[0m'

def main():
    print(f"\n{BOLD}---- FRED DATA ACCESS ----{RESET}\n")
    print(f"Select an option:\n")
    print(f"1) Fetch a list of economic data series from FRED.")
    print(f"2) Load the economic data series from FRED with the tags.txt file.")
    print(f"3) Exit Program")

    choice = input(f"\nEnter your selection: ").strip()
    
    if choice == "1":
        tags = input(f"\nList the tags for FRED data series, separated by commas: ").upper().split(",")
        tags = [tag.strip() for tag in tags] # Remove leading/trailing spaces from each tag
    
    elif choice == "2":
        try: 
            with open("tags.txt","r") as file:
                tags = [line.strip().upper() for line in file.readlines() if line.strip()]
            
            if not tags:
                print(f"\n{RED}Error: tags.txt is empty!{RESET}")

            else:
                print(f"\nLoading FRED data series for tags: {tags}")
        
        except FileNotFoundError:
            print(f"\n{RED}Error: tags.txt file not found!{RESET}")


    elif choice == "3":
        print(f"\nExiting Program... Good bye!")
        return None

    # Get API Key
    fred_api_key = get_key()

    # Initiate Empty Dataframes
    metadata_df = pd.DataFrame()
    time_series_df = pd.DataFrame()

    # Track invalid tags
    invalid_tags = []

    for tag in tags: # Iterate through list of tags
        print(f"\nFetching data for {tag}...")
        
        try: # Get metadata for tag       
            meta = get_metadata(tag,fred_api_key)

                # If metadata is missing or empty, track in invalid_tags
            if meta is None or meta.empty:
                print(f'Empty Metadata for tag: {tag}')
                invalid_tags.append(tag)

            else: # If available, add to metadata_df
                metadata_df = pd.concat([metadata_df, meta], ignore_index=True)
                
        # When an unexpected exception occurs, track in invalid_tags        
        except Exception as e:
            print(f'Error fetching metadata for {tag}: {str(e)}')
            invalid_tags.append(tag)

        try:# Get time series for tag       
            series = fetch_fred_series(tag, fred_api_key,start_date='1999-12-31')

            # If a time series is missing or empty, track in invalid tags
            if series is None or series.empty:
                print(f"Series data missing for tag: {tag}")
                invalid_tags.append(tag)

            else: # If available, add to time_series_df
                time_series_df = pd.concat([time_series_df,series],ignore_index=True)
        
        # When an unexpected exception occurs, track in invalid tags
        except Exception as e:
            print(f"Error fetching records for {tag}: {str(e)}")
            invalid_tags.append(tag)

    print(f"\nInvalid tags: {invalid_tags}\n")
    # print(metadata_df.info())
    # print("\n")
    # print(time_series_df.info())

# Format the Data
    try:

        # Define columns to drop
        drop_meta_cols = ['realtime_start','realtime_end','last_updated']
        drop_series_cols = ['realtime_start','realtime_end']

        # Check if dropped columns are valid (returns true if valid)
        valid_meta_drops = all(col in metadata_df.columns for col in drop_meta_cols)
        valid_series_drops = all(col in time_series_df.columns for col in drop_series_cols)

        # Drop colums or raise an error
        if valid_meta_drops and valid_series_drops:
            cleaning_metadata = metadata_df.drop(columns=drop_meta_cols)
            cleaning_time_series = time_series_df.drop(columns=drop_series_cols)
        else:
            raise KeyError("Attempt to drop columns was unsuccessful, please review column names in the DataFrames.")

    except Exception as e:

        print(f"Unexpected Error: {e}")
    
    # Add source column as FRED in Metadata
    cleaning_metadata['source'] = 'FRED'

    # Convert data types to dates and numbers
    cleaning_metadata['observation_start'] = pd.to_datetime(cleaning_metadata['observation_start'])
    cleaning_metadata['observation_end'] = pd.to_datetime(cleaning_metadata['observation_end'])
    cleaning_metadata['popularity'] = cleaning_metadata['popularity'].astype(int)

    cleaning_time_series['date'] = pd.to_datetime(cleaning_time_series['date'])
    cleaning_time_series['value'] = pd.to_numeric(cleaning_time_series['value'])

    cleaning_time_series = cleaning_time_series.rename(columns={'value':'obs'})


    clean_metadata = cleaning_metadata
    clean_time_series = cleaning_time_series

    # Review DataFrames
    print("Metadata")
    for column in clean_metadata.columns:
        print(f'{column}: {clean_metadata[column].iloc[0]}')
    print('\n')
    print(clean_metadata.info())
    print('\n')
    print("Time Series")
    for column in clean_time_series.columns:
        print(f'{column}: {clean_time_series[column].iloc[0]}')
    print('\n')   
    print(clean_time_series.info())
    print('\n')

    # Connect to Database and Upload Data
    # Connection Parameters
    host = 'db'
    port = '5432'
    user = 'postgres'
    password = 'postgres'
    default_database = 'postgres'
    database = 'fred_data'

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{default_database}', isolation_level="AUTOCOMMIT")
    print(f"\nTesting connection to PostgreSQL database...")

    # Test the connection
    try:
        with engine.connect() as connection:
            print(f"\nConnection successful!")
            result = connection.execute(text("SELECT version();"))
            print(f"\nPostgreSQL version:", result.fetchone())

            # Check if fred_data exists
            check_fred = connection.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{database}'"))
            
            # If not, CREATE  it
            if not check_fred.scalar():             
                connection.execute(text(f'CREATE DATABASE {database}'))
                print(f"\nDatabase '{database}' created.")

            # Otherwise, verify it already exists    
            else: 
                print(f"\nDatabase '{database}' already exists.")
    
    except Exception as e:
        print(f"\nError connecting to the database:", e)
    
    # Close connection to the default_database
    engine.dispose()

    # Define tables required for fred_data
    metadata_table_definition = """
        id VARCHAR(20) PRIMARY KEY,
        title TEXT,
        observation_start DATE,
        observation_end DATE,
        frequency VARCHAR(20),
        frequency_short VARCHAR (5),
        units TEXT,
        units_short TEXT,
        seasonal_adjustment TEXT,
        seasonal_adjustment_short TEXT,
        popularity INTEGER,
        notes TEXT,
        source TEXT
        """

    time_series_table_definition = """
        date DATE,
        obs NUMERIC,
        id VARCHAR(20),
        PRIMARY KEY (id, date),
        FOREIGN KEY(id) REFERENCES metadata(id)
        """

    # Reconnect to fred_data
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}')

    try:
        with engine.connect() as connection:
            print(f"\nConnected to '{database}' successfully!")
            # result = connection.execute(text("SELECT current_database();"))
            # print(f"\nCurrent database: {result.fetchone()[0]}")

            # Create the table for metadata 
            create_table(connection, 'metadata', metadata_table_definition)
            print(f"\nMeta table created")

            # Create the table for time_series
            create_table(connection, 'time_series', time_series_table_definition)
            print(f"\nTime Series table created")

    except Exception as e:
        print(f"\nError connecting to '{database}':", e)
        
    # Import the fred data to sql database
    try:
        clean_metadata.to_sql('metadata', engine, if_exists='append', index=False,
                              dtype={
                                  'id': types.VARCHAR(20),
                                  'title': types.TEXT,
                                  'observation_start': types.DATE,
                                  'observation_end': types.DATE,
                                  'frequency': types.VARCHAR(20),
                                  'frequency_short': types.VARCHAR(5),
                                  'units': types.TEXT,
                                  'units_short': types.TEXT,
                                  'seasonal_adjustment': types.TEXT,
                                  'seasonal_adjustment_short': types.TEXT,
                                  'popularity': types.INTEGER,
                                  'notes': types.TEXT,
                                  'source': types.TEXT
                              })
        print(f"{GREEN}Metadata inserted successfully!{RESET}")

    except Exception as e:
        print(f"{RED}Error inserting metadata: {str(e)}{RESET}")

    try:
        clean_time_series.to_sql('time_series', engine, if_exists='append', index=False,
                                 dtype={
                                    'date': types.DATE,
                                    'obs': types.NUMERIC,
                                    'id': types.VARCHAR(20)       
                                 })
        print(f"{GREEN}Time series data inserted successfully!{RESET}")

    except Exception as e:
        print(f"{RED}Error inserting time series data: {str(e)}{RESET}")

    connection.commit()
    connection.close()

    
if __name__ == "__main__":
    main()