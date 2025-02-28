# main.py

import os
import pandas as pd

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
        print(tags)

    elif choice == "2":
        print(f"\nExiting Program... Good bye!")

    all_data = pd.DataFrame()
    



    

if __name__ == "__main__":
    main()