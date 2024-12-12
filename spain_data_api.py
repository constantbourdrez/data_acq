import requests
import json
import os
from urllib.parse import quote

BASE_URL = "http://datos.gob.es/apidata"

def fetch_datasets():
    """Fetch the list of datasets available in the catalog."""
    url = f"{BASE_URL}/catalog/dataset/title/distribution%20of%20the%20number%20of%20doctors?_sort=title&_pageSize=10&_page=0"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching datasets: {e}")
        return None

def retrieve_and_save_data(dataset_id, output_file):
    """Retrieve the data for a specific dataset using its ID and save it to a CSV file."""
    # Encode the dataset_id to be properly URL-safe
    encoded_id = quote(dataset_id)
    url = f"{BASE_URL}/catalog/dataset/{encoded_id}"

    try:
        # Make the request to fetch dataset details
        response = requests.get(url)
        response.raise_for_status()
        dataset_details = response.json()

        # Get the 'first' URL from the dataset details and fetch the actual data
        first_url = dataset_details['result'].get('first', '')
        if first_url:
            # Fetch the data from the 'first' URL (this should contain the actual dataset)
            all_data = []
            while first_url:
                data_response = requests.get(first_url)
                data_response.raise_for_status()
                data = data_response.json()

                # Add data to the list
                if isinstance(data, list):
                    all_data.extend(data)

                # Check if there is a next page and update the URL
                first_url = dataset_details['result'].get('next', '')

            # Create the folder structure if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            # Write the dataset to a CSV file
            if all_data:
                # Assuming each entry is a dictionary, get the keys (column names) from the first entry
                keys = all_data[0].keys()

                with open(output_file, 'w', newline='', encoding='utf-8') as file:
                    file.write(','.join(keys) + '\n')
                    for row in all_data:
                        file.write(','.join(str(row[key]) for key in keys) + '\n')

                print(f"Data saved to {output_file}")
            else:
                print(f"No data found for {dataset_id}")
        else:
            print(f"No 'first' URL found in dataset details.")
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving data for dataset {dataset_id}: {e}")

if __name__ == "__main__":
    print("Fetching list of available datasets...")
    datasets = fetch_datasets()
    print(datasets["result"]["items"][0]["distribution"][0])
    # if datasets:
    #     results = datasets.get("result", {}).get("items", [])

    #     if not results:
    #         print("No datasets found in 'results'. Exiting script.")
    #     else:
    #         print(f"Found {len(results)} datasets. Processing...")

    #         # Process each dataset
    #         for dataset in results:
    #             # Extract the dataset ID from the response (e.g., "/tpx/Sociedad_2589/Salud_2590/EPSC_5623/a2023_11192/l0/s01003.px")
    #             dataset_id = dataset.get("identifier", "").strip()

    #             if not dataset_id:
    #                 print(f"No valid dataset ID found for dataset. Skipping.")
    #                 continue

    #             # Extract the title, which is a list of dictionaries
    #             titles = dataset.get("title", [])
    #             title = ""
    #             if isinstance(titles, list):
    #                 # Extract the '_value' from the first element (assuming the title is in the first entry)
    #                 title = next((t["_value"] for t in titles if "_value" in t), "No Title")

    #             print(f"Processing dataset: {title} (ID: {dataset_id})")

    #             # Specify the output CSV file for each dataset
    #             output_file = f"data/spain/{title.replace(' ', '_')}_data.csv"

    #             # Retrieve and save the data for the current dataset
    #             retrieve_and_save_data(dataset_id, output_file)
    # else:
    #     print("No datasets found. Exiting script.")
