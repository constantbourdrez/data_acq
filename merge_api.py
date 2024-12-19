import csv

def standardize_columns(data):
    for entry in data:
        for key, value in entry.items():
            if isinstance(value, str):
                entry[key] = value.strip()
    return data

def clean_column_names(data):

    for entry in data:
        entry = {key.strip().replace('\ufeff', '').replace('"', ''): value for key, value in entry.items()}
    return data

def load_csv(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8-sig') as file:  # 'utf-8-sig' handles BOM
        reader = csv.DictReader(file)


        fieldnames = [name.strip().replace('\ufeff', '').replace('"', '') for name in reader.fieldnames]

        data = []
        for row in reader:
            cleaned_row = {fieldnames[i]: value for i, value in enumerate(row.values())}
            data.append(cleaned_row)


    return data

def filter_data(data):
    # Ensure the 'annee' field is accessed correctly
    return [entry for entry in data if int(entry['annee']) > 2022]

# Save data back to CSV
def save_csv(file_path, data, fieldnames):
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


# Merge two datasets by matching keys
def merge_datasets(data1, data2, keys):
    merged = {}
    for entry in data2:
        key = tuple(entry[k] for k in keys)
        merged[key] = entry


    result = []
    for entry in data1:
        key = tuple(entry[k] for k in keys)
        if key in merged:
            merged_entry = merged[key]
            merged_data = {**entry, **merged_entry}
            result.append(merged_data)

    return result

# Handle missing data by filling empty values
def handle_missing_data(data, numeric_columns, categorical_columns):
    for entry in data:
        for column in numeric_columns:
            if column not in entry or entry[column] == '':
                entry[column] = '0'  #
        for column in categorical_columns:
            if column not in entry or entry[column] == '':
                entry[column] = 'Unknown'
    return data

def remove_column(data, column_name):
    return [{k: v for k, v in row.items() if k != column_name} for row in data]


def main():

    effectif_file = 'data/dataset_export_demographie-effectifs-et-les-densites.csv'
    prescription_file = 'data/dataset_export_prescriptions.csv'
    honoraire_file = 'data/dataset_export_honoraires-detailles.csv'

    print("Loading data...")

    ameli_effectif = load_csv(effectif_file)
    ameli_prescription = load_csv(prescription_file)
    ameli_honoraire = load_csv(honoraire_file)

    print("Filtering data...")
    ameli_effectif = filter_data(ameli_effectif)
    ameli_prescription = filter_data(ameli_prescription)
    ameli_honoraire = filter_data(ameli_honoraire)

    print("Standardizing columns...")

    ameli_effectif = standardize_columns(ameli_effectif)
    ameli_prescription = standardize_columns(ameli_prescription)
    ameli_honoraire = standardize_columns(ameli_honoraire)

    print("Merging datasets...")
    keys = ['annee', 'profession_sante', 'departement', 'region', 'libelle_region', 'libelle_departement']

    # Merge ameli_honoraire and ameli_prescription
    merged_df_1 = merge_datasets(ameli_honoraire, ameli_prescription, keys)

    # Merge the result with ameli_effectif
    merged_df_2 = merge_datasets(merged_df_1, ameli_effectif, keys)

    print("Handling missing data...")
    # 4. Handle missing data
    numeric_columns = ['honoraires', 'prescriptions', 'effectif']
    categorical_columns = ['profession_sante', 'departement', 'region', 'libelle_region', 'libelle_departement']

    final_data = handle_missing_data(merged_df_2, numeric_columns, categorical_columns)
    print(final_data[0].keys())
    print("Removing 'densite' column...")
    cleaned_data = remove_column(final_data, 'densite')

    print("Saving final data...")
    # Save the final data to a new CSV file
    fieldnames = cleaned_data[0].keys()  # Get fieldnames from the first entry in the final data
    save_csv('data/final_ameli.csv', cleaned_data, fieldnames)

    print("Data processing completed. Final data saved to 'final_data.csv'.")

if __name__ == "__main__":
    main()
