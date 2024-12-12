import requests
import json
from api_key import headers

def fetch_data():
    # Define the API URL endpoints and filenames
    endpoints = {
        "Device": "dataset_device.json",
        "HealthcareService": "dataset_healthcare_service.json",
        "Organization": "dataset_organization.json",
        "Practitioner": "dataset_practitioner.json",
        "PractitionerRole": "dataset_practitioner_role.json"
    }


    # Loop through each endpoint and fetch data
    for endpoint, filename in endpoints.items():
        url = f"https://gateway.api.esante.gouv.fr/fhir/{endpoint}"
        print(f"Fetching data from {url}...")

        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the response is successful
        if response.status_code == 200:
            # Parse the response JSON
            data = response.json()

            # Save the data to the corresponding file
            with open(filename, 'w') as file:
                json.dump(data, file, indent=4)
            print(f"Data saved to {filename}")
        else:
            print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
            print("Response:", response.text)

if name == "__main__":
    fetch_data()
