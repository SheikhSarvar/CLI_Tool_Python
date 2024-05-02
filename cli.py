import sys
import pandas as pd
import geocoder
import time
import os
import warnings
import re
import numpy as np
warnings.filterwarnings('ignore')


def valid_df_row(df):
    return  df[df['Email'].apply(lambda x: bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(x)))) &
              ~df['First Name'].apply(lambda x: str(x).isdigit()) &
              ~df['Last Name'].apply(lambda x: str(x).isdigit()) & 
              df['Residential Address Postcode'].apply(lambda x: str(x).isdigit()) & 
              df['Postal Address Postcode'].apply(lambda x: str(x).isdigit())]


def clean_df(df):
    # Drop rows with missing values
    df.dropna(inplace=True)
    df['Postal Address Postcode'] = df['Postal Address Postcode'].astype(str).str.replace('.0', '')
    df['Residential Address Postcode'] = df['Residential Address Postcode'].astype(str)
    # Concatenate address components
    df['Full_Residential_Address'] = df['Residential Address Locality'] + ', ' + df['Residential Address State'] + ', ' + df['Residential Address Postcode']
    df['Full_Postal_Address'] = df['Postal Address Locality'] + ', ' + df['Postal Address State'] + ', ' + df['Postal Address Postcode']

    return df



def geocode_address(address):
    num_retries = 0
    retry=3
    while num_retries < retry:
        location = geocoder.osm(address)
        try:
            if location.ok:
                return (location.lat, location.lng)
            else:
                print(f"Failed to geocode address: {address}")
                return np.nan
        except Exception as e:
            print(f"Error geocoding address {address}: {e}")
            num_retries += 1
            time.sleep(0.5)
            continue
    print(f'failed after retry for this add, {address}')
    return np.nan
    

def user_valid(csv_file):
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} does not exist.")
        sys.exit(1)
    _, file_extension = os.path.splitext(csv_file)
    if file_extension.lower() not in ['.csv', '.xlsx']:
        print("Error: File type not supported. Expected .csv or .xlsx")
        sys.exit(1)

    df = pd.read_csv(csv_file) if file_extension.lower() == '.csv' else pd.read_excel(csv_file)
    df = valid_df_row(df)
    df = clean_df(df)
    batches = [df['Full_Postal_Address'][i:i+1000] for i in range(0, len(df), 1000)]
    results = []
    for i, batch in enumerate(batches):
        batch_results = [geocode_address(address) for address in batch]
        results.extend(batch_results)
    df['Postal_co-ordinates'] = pd.Series(results)
    df.dropna(inplace=True)
    df.to_csv('clean_client.csv', index=False)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <csv_file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    user_valid(csv_file)
