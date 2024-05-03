import sys
import pandas as pd
import geocoder
from concurrent.futures import ThreadPoolExecutor
import time
import os
import warnings
import re
import numpy as np
warnings.filterwarnings('ignore')


def valid_df_row(df):
    # Check if the row is valid by checking if the email is valid, if the first name and last name are not numbers,
    # if the residential address postcode is a number, and if the postal address postcode is a number
    return  df[df['Email'].apply(lambda x: bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(x)))) &
              ~df['First Name'].apply(lambda x: str(x).isdigit()) &
              ~df['Last Name'].apply(lambda x: str(x).isdigit()) & 
              df['Residential Address Postcode'].apply(lambda x: str(x).isdigit()) & 
              df['Postal Address Postcode'].apply(lambda x: str(x).isdigit())]



def clean_df(df):
    # Drop rows with missing values
    df.dropna(inplace=True)

    # correcting syntax for geocoding (removing trailling zero)
    df['Residential Address Postcode'] = df['Residential Address Postcode'].apply(lambda x: str(int(float(x))))
    df['Postal Address Postcode'] = df['Postal Address Postcode'].apply(lambda x: str(int(float(x))))

    # Concatenate address components for geocoding the address
    df['Full_Residential_Address'] = df['Residential Address Locality'] + ', ' + df['Residential Address State'] + ', ' + df['Residential Address Postcode']
    df['Full_Postal_Address'] = df['Postal Address Locality'] + ', ' + df['Postal Address State'] + ', ' + df['Postal Address Postcode']

    # Droping duplicate values for same email address
    df.drop_duplicates(subset=['Email'], inplace=True)

    # Droping non-ASCII char
    lst=['Residential Address Street', 'Residential Address Locality', 'Residential Address State',
                    'Postal Address Street', 'Postal Address Locality', 'Postal Address State']
    for col in lst:
        df[col] = df[col].apply(lambda x: ''.join(char for char in x if ord(char) < 128))
    
    # Return cleaned dataframe
    return df


# this function takes address from the dataframe column and geocodes it
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
    

def process_batch(batch, results_list):
    batch_results = [geocode_address(address) for address in batch]
    results_list.extend(batch_results)


def user_valid(csv_file):
    # Checking if file exists
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} does not exist.")
        sys.exit(1)
    
    # Checking file extension
    _, file_extension = os.path.splitext(csv_file)
    if file_extension.lower() not in ['.csv', '.xlsx']:
        print("Error: File type not supported. Expected .csv or .xlsx")
        sys.exit(1)
    
    # Reading csv file
    df = pd.read_csv(csv_file) if file_extension.lower() == '.csv' else pd.read_excel(csv_file)

    # Checking dataframe is Not Null
    if df.empty:
        print("Error: Dataframe is empty.")
        sys.exit(1)
    
    # checking dataframe columns 
    lst=['Email', 'First Name', 'Last Name', 'Residential Address Street',
       'Residential Address Locality', 'Residential Address State',
       'Residential Address Postcode', 'Postal Address Street',
       'Postal Address Locality', 'Postal Address State',
       'Postal Address Postcode']
    if not all(col in df.columns for col in lst):
        print("Error: Dataframe columns are not correct.")
        sys.exit(1)

    
    # Cleaning dataframe
    df = clean_df(df)

    # Validation dataframe values as per columns
    df=valid_df_row(df)

    # Creating batches for largesize dataset
    if len(df) > 0:
        batch_size = min(1000, len(df))
        batches_Postal = [df['Full_Postal_Address'][i:i+batch_size] for i in range(0, len(df), batch_size)]
        batches_Resident=[df['Full_Residential_Address'][i:i+batch_size] for i in range(0, len(df), batch_size)]
    else:
        batches_Postal = []
        batches_Resident=[]

    # Geocoding addresses and storing to dataframe                        
    results_Postal = []
    results_Resident = []

    # Create a thread pool
    with ThreadPoolExecutor() as executor:
    # Submit tasks for each batch of addresses
        postal_futures = [executor.submit(process_batch, batch_postal, results_Postal) for batch_postal in batches_Postal]
        resident_futures = [executor.submit(process_batch, batch_resident, results_Resident) for batch_resident in batches_Resident]

# Wait for all tasks to complete
    for future in postal_futures + resident_futures:
        future.result()

# Now you can proceed to assign results to DataFrame
    df['Postal_co-ordinates'] = pd.Series(results_Postal)
    df['Residential_co-ordinates'] = pd.Series(results_Resident)


    # droping Null Geocoding address raws
    df.dropna(inplace=True)

    # Deleting additional created column
    df.drop(columns=['Full_Postal_Address'], inplace=True)
    df.drop(columns=['Full_Residential_Address'], inplace=True)

    #  saving to csv 
    output_file=df.to_csv('clean_client.csv', index=False)
    print(f"Cleaned data saved to: {output_file}")


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <csv_file>")
        sys.exit(1)
    
    csv_file = sys.argv[1]
    user_valid(csv_file)
