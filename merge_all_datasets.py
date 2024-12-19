import csv
from tqdm import tqdm


print("Loading profession mapping...")
profession_mapping = {}
with open('profession_mapping.csv', mode='r') as p_file:
    reader = csv.DictReader(p_file)
    for row in reader:
        profession_mapping[row['e_sante_profession']] = row['profession_sante']


print("Loading and filtering ameli_dataset...")
ameli_file_path = 'data/ameli_preprocessed.csv'
ameli_headers = None
filtered_ameli_rows = []

with open(ameli_file_path, mode='r') as a_file:
    reader = csv.DictReader(a_file)
    ameli_headers = reader.fieldnames
    for row in reader:
        filtered_ameli_rows.append(row)

print(f"Filtered ameli_dataset: {len(filtered_ameli_rows)} rows remaining.")


print("Processing e_sante_dataset and creating lookup dictionary...")
e_sante_data = {}

with open('data/practitioner.csv', mode='r') as e_file:
    reader = csv.DictReader(e_file, delimiter=',')  # Use '|' as the delimiter
    for row in reader:
        if 'Code commune (coord. structure)' in row and row['Code commune (coord. structure)']:
            prefix = row['Code commune (coord. structure)'][:2]
            if prefix not in e_sante_data:
                e_sante_data[prefix] = []
            # Apply profession mapping
            if 'qualification' in row:
                original_profession = row['qualification']
                mapped_profession = profession_mapping.get(original_profession, original_profession)
                row['Libellé profession_2'] = mapped_profession
            e_sante_data[prefix].append(row)

print(f"Processed e_sante_dataset: {len(e_sante_data)} unique prefixes.")


print("Merging datasets with fallback and saving directly to disk...")


merged_headers = list(ameli_headers) + list(next(iter(e_sante_data.values()))[0].keys())


used_e_sante_rows = set()

with open('data/doctor_dataset.csv', mode='w', newline='') as m_file:
    writer = csv.DictWriter(m_file, fieldnames=merged_headers)
    writer.writeheader()

    total_columns = [col for col in ameli_headers if 'total' in col.lower()]
    used_ameli_rows = set()


    for dept_code, e_sante_rows in tqdm(e_sante_data.items()):
        # Filter ameli_rows by department code
        matching_ameli_rows = [row for row in filtered_ameli_rows if row['departement'] == dept_code]

        for e_sante_row in e_sante_rows:
            found_match = False
            for ameli_row in matching_ameli_rows:
                row_id = id(ameli_row)

                # Check if the row has already been used
                if row_id not in used_ameli_rows and e_sante_row.get('Libellé profession_2') == ameli_row.get('profession_sante'):
                    used_ameli_rows.add(row_id)  # Mark the row as used
                    found_match = True

                    # Merge rows
                    merged_row = {**e_sante_row, **ameli_row}

                  
                    for col in total_columns:
                        if col in merged_row and merged_row[col].replace('.', '', 1).isdigit():
                            merged_row[col] = str(float(merged_row[col]) / len(filtered_ameli_rows))


                    writer.writerow(merged_row)
                    break


            if not found_match:

                pass


    # Add remaining e_sante rows that were not matched
    print("Processing unmatched e_sante rows...")
    for dept_code, e_sante_rows in tqdm(e_sante_data.items()):
        for e_sante_row in e_sante_rows:
            row_id = id(e_sante_row)
            if row_id not in used_e_sante_rows:
                used_e_sante_rows.add(row_id)
                # Add placeholders for ameli data
                unmatched_row = {col: '' for col in ameli_headers}
                merged_row = {**unmatched_row, **e_sante_row}

                writer.writerow(merged_row)

print("Merging completed. All rows mapped or included with placeholders. Dataset saved successfully as 'merged_dataset.csv'.")
