import csv
import re
import os
import sys
import geocoder
import time
from concurrent.futures import ThreadPoolExecutor


def read_file(input_file):
    # checking file path
    if not os.path.exists(input_file):
        print(f"Error: {input_file} does not exist.")
        sys.exit(1)

    # Checking file extension
    _, file_extension = os.path.splitext(input_file)
    if file_extension.lower() not in ['.csv', '.xlsx']:
        print("Error: File type not supported. Expected .csv or .xlsx")
        sys.exit(1)

    #  Open input file using csv
    with open(input_file, newline='', encoding='utf-8-sig') as csvfile:
        # Create a CSV reader object
        reader = csv.DictReader(csvfile)
        data = [row for row in reader]

    # Checking data is Not Null
        if not data:
            print("Error: Data is empty.")
            sys.exit(1)
        # Checking data columns
        if len(data[0]) != 11:
            print("Error: Data columns are not correct.")
            sys.exit(1)
    return data 

def valid_row(row):
    # Check if the row is valid by checking if the email is valid, if the first name and last name are not numbers,
    # if the residential address postcode is a number, and if the postal address postcode is a number
    return bool(re.match(r"[^@]+@[^@]+\.[^@]+", str(row['Email']))) and not str(row['First Name']).isdigit() and not str(row['Last Name']).isdigit() and \
           str(row['Residential Address Postcode']).isdigit() and \
           str(row['Postal Address Postcode']).isdigit()

def geocode_address(address):
    num_retries = 0
    retry = 3
    while num_retries < retry:
        location = geocoder.osm(address)
        try:
            if location.ok:
                return (location.lat, location.lng)
            else:
                print(f"Failed to geocode address: {address}")
                return None
        except Exception as e:
            print(f"Error geocoding address {address}: {e}")
            num_retries += 1
            time.sleep(0.5)
            continue
    print(f'failed after retry for this add, {address}')
    return None

def process_batch(batch, results_list):
    batch_results = [geocode_address(address) for address in batch]
    results_list.extend(batch_results)
        
def write_to_csv(data, output_file):
    with open(output_file, 'w', newline='') as file:
        columns = ['Email', 'First Name', 'Last Name', 'Residential Address Street',
                      'Residential Address Locality', 'Residential Address State',
                      'Residential Address Postcode', 'Postal Address Street',
                      'Postal Address Locality', 'Postal Address State',
                      'Postal Address Postcode', 'Postal_co-ordinates', 'Residential_co-ordinates']
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        for row in data:
            # Check if any values in the row are non-empty
            if any(value is not None and value == 'NaN' for value in row.values()):
                continue
                # Filter out extra fields not present in the columns list
            filtered_row = {k: v for k, v in row.items() if k in columns}
            writer.writerow(filtered_row)


def main(input_file):

    # Read data from input CSV file
    data = read_file(input_file)
    
    # Clean the data
    data = [row for row in data if valid_row(row)]

    # Separate residential and postal addresses
    res_add = [(', '.join([row['Residential Address Locality'], row['Residential Address State'], 
                           row['Residential Address Postcode']])) for row in data]
    
    pos_add = [(', '.join([row['Postal Address Locality'],row['Postal Address State'], 
                           row['Postal Address Postcode']])) for row in data]

    # Creating batches for large size dataset
    if len(data) > 0:
        batch_size = min(1000, len(data))
        batches_postal = [res_add[i:i + batch_size] for i in range(0, len(data), batch_size)]
        batches_resident = [pos_add[i:i + batch_size] for i in range(0, len(data), batch_size)]
    else:
        batches_postal = []
        batches_resident = []

    # Geocoding addresses and storing to outputfile
    results_postal = []
    results_resident = []

    # Create a thread pool
    with ThreadPoolExecutor() as executor:
        # Submit tasks for each batch of addresses
        postal_futures = [executor.submit(process_batch, batch_postal, results_postal) for batch_postal in
                          batches_postal]
        resident_futures = [executor.submit(process_batch, batch_resident, results_resident) for batch_resident in
                            batches_resident]

    # Wait for all tasks to complete
    for future in postal_futures + resident_futures:
        try:
            future.result()
        except Exception as e:
            print(f"Error: {e}")
    
    # Merge coordinates for output
    for i, row in enumerate(data):
        row['Postal_co-ordinates'] = results_postal[i]
        row['Residential_co-ordinates'] = results_resident[i]

    # Drop null values from data
    data = [row for row in data if all(row)]

    # Saving the output_data
    write_to_csv(data, 'output_file.csv')

    print("Cleaned data saved")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python script.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]
    main(csv_file)


