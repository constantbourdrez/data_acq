import requests
import json
import os

BASE_URL = "https://data.ameli.fr/api/explore/v2.1"

def fetch_datasets():
    """Fetch the list of datasets available in the catalog."""
    url = f"{BASE_URL}/catalog/datasets"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching datasets: {e}")
        return None

def save_datasets_to_file(datasets):
    """Save the dataset descriptions and IDs to a file."""
    if not datasets:
        print("No datasets to save.")
        return

    output_dir = "datasets_list"
    os.makedirs(output_dir, exist_ok=True)
    filename = os.path.join(output_dir, "datasets_list.json")

    # Extract dataset_id and description
    cleaned_data = []
    for dataset in datasets.get("results", []):
        dataset_info = {
            "dataset_id": dataset.get("dataset_id"),
            "description": dataset.get("metas", {}).get("default", {}).get("description"),
        }
        cleaned_data.append(dataset_info)

    # Save to JSON file
    with open(filename, "w", encoding="utf-8") as file:
        json.dump(cleaned_data, file, indent=4, ensure_ascii=False)

    print(f"Datasets list saved to {filename}")

if __name__ == "__main__":
    print("Fetching list of available datasets...")
    datasets = fetch_datasets()
    if datasets:
        save_datasets_to_file(datasets)

        # List of keywords for filtering
        keywords = [
            'honoraires', 'demographie-effectifs-et-les-densites', 'effectifs',
            'honoraires-detailles', 'prescriptions']
        for dataset in datasets.get("results", []):
            dataset_id = dataset.get("dataset_id")
            if dataset_id in keywords:
                print(f"Exporting dataset {dataset_id} to CSV...")
                url = f"https://data.ameli.fr/api/explore/v2.1/catalog/datasets/{dataset_id}/exports/csv"

                # Optional parameters
                params = {
                    'delimiter': ',',
                    'quote_all': True,
                    'with_bom': True,
                }

                try:
                    response = requests.get(url, params=params)
                    response.raise_for_status()  # Check for HTTP errors

                    # Save the CSV file
                    with open(f"data/dataset_export_{dataset_id}.csv", "wb") as f:
                        f.write(response.content)

                    print("Dataset successfully exported to CSV.")
                except requests.exceptions.RequestException as e:
                    print(f"Error exporting dataset: {e}")
    else:
        print("No datasets found.")
