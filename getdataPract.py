import requests
import csv
import json
import pandas as pd
from api_key import headers

url_practitioner = "https://gateway.api.esante.gouv.fr/fhir/Practitioner"
url_practitioner_role = "https://gateway.api.esante.gouv.fr/fhir/PractitionerRole"

rpps_data = pd.read_csv('data/rpps.csv', encoding='utf-8')
num_pages_max = 2

def preprocess_diploma(input_file, specialty_key="TRE_R42_DESCnonQualifiant", diploma_key="TRE_R48_DiplomeEtatFrancais"):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    specialty_mapping = {}
    diploma_mapping = {}

    if specialty_key in data:
        for entry in data[specialty_key]:
            code = entry['code']
            meaning = entry['meaning']
            specialty_mapping[code] = meaning

    if diploma_key in data:
        for entry in data[diploma_key]:
            code = entry['code']
            meaning = entry['meaning']
            diploma_mapping[code] = meaning

    return specialty_mapping, diploma_mapping

input_file = 'data/profession_diploma.json'
specialty_mapping, diploma_mapping = preprocess_diploma(input_file)

def map_specialty(code, specialty_mapping):
    return specialty_mapping.get(code, code)

def map_diploma(code, diploma_mapping):
    return diploma_mapping.get(code, code)

def clean_and_map_v3(merged_data, specialty_mapping, diploma_mapping):
    cleaned_data = []

    for entry in merged_data:
        cleaned_entry = {}

        try:
            entry['extension'] = json.loads(entry.get('extension', '[]').replace("'", '"'))
            entry['specialty'] = json.loads(entry.get('specialty', '[]').replace("'", '"'))
            entry['identifier'] = json.loads(entry.get('identifier', '[]').replace("'", '"'))
            entry['qualification'] = json.loads(entry.get('qualification', '[]').replace("'", '"'))
        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding JSON for entry {entry['id']}: {e}")
            entry['extension'] = []
            entry['specialty'] = []
            entry['identifier'] = []
            entry['qualification'] = []

        extension = entry.get('extension', [])
        if isinstance(extension, list) and len(extension) > 0 and isinstance(extension[0], dict):
            given_name = ' '.join(extension[0].get('valueHumanName', {}).get('given', []))
        else:
            given_name = ""
        cleaned_entry['given_name'] = given_name

        if isinstance(extension, list) and len(extension) > 0 and isinstance(extension[0], dict):
            suffix = ' '.join(extension[0].get('valueHumanName', {}).get('suffix', []))
        else:
            suffix = ""
        cleaned_entry['suffix'] = suffix

        if isinstance(extension, list) and len(extension) > 0 and isinstance(extension[0], dict):
            family_name = extension[0].get('valueHumanName', {}).get('family', "")
        else:
            family_name = ""
        cleaned_entry['family_name'] = family_name

        cleaned_entry['active'] = entry.get('active', False)

        specialties = entry.get('specialty', [])
        cleaned_specialties = []
        for specialty in specialties:
            if 'coding' in specialty:
                for coding in specialty['coding']:
                    if coding.get('code'):
                        mapped_code = map_specialty(coding['code'], specialty_mapping)
                        cleaned_specialties.append(mapped_code)
        cleaned_entry['specialty'] = cleaned_specialties

        qualifications = entry.get('qualification', [])
        cleaned_qualifications = []
        for qualification in qualifications:
            if 'code' in qualification:
                coding = qualification['code'].get('coding', [])
                for cod in coding:
                    if cod.get('code'):
                        mapped_code = map_diploma(cod['code'], diploma_mapping)
                        cleaned_qualifications.append(mapped_code)
        cleaned_entry['qualification'] = cleaned_qualifications

        identifiers = entry.get('identifier', [])
        cleaned_identifiers = []
        for identifier in identifiers:
            if isinstance(identifier, dict):
                cleaned_identifier = identifier.get('value')
                if cleaned_identifier:
                    cleaned_identifiers.append(cleaned_identifier)
        cleaned_entry['identifier'] = cleaned_identifiers

        name = entry.get('name', [])
        if isinstance(name, list) and len(name) > 0 and isinstance(name[0], dict):
            prefix = ' '.join(name[0].get('prefix', []))
        else:
            prefix = ""
        cleaned_entry['prefix'] = prefix

        cleaned_entry['id'] = entry.get('id')
        cleaned_entry['active'] = entry.get('active')

        cleaned_data.append(cleaned_entry)

    return cleaned_data

def fetch_data(url, num_pages_max=num_pages_max):
    all_data = []
    next_url = url
    while next_url and num_pages_max > 0:
        print(f"Fetching data from: {next_url}")
        response = requests.get(next_url, headers=headers)
        num_pages_max -= 1

        if response.status_code != 200:
            print(f"Erreur: {response.status_code} - {response.text}")
            break

        data = response.json()

        if "entry" in data:
            all_data.extend(data["entry"])

        next_url = None
        if "link" in data:
            for link in data["link"]:
                if link["relation"] == "next":
                    next_url = link["url"]
                    break

    return all_data

practitioners_data = fetch_data(url_practitioner)
practitioners_role_data = fetch_data(url_practitioner_role)

practitioners_dict = {entry['resource']['id']: entry['resource'] for entry in practitioners_data}

merged_data = []
for entry in practitioners_role_data:
    practitioner_id = entry['resource']['practitioner']['reference'].split("/")[1]
    if practitioner_id in practitioners_dict:
        practitioner_data = practitioners_dict[practitioner_id]
        merged_entry = practitioner_data.copy()
        merged_entry.update(entry['resource'])
        filtered_entry = {
            'extension': str([{"valueHumanName": ext.get("valueHumanName")} for ext in merged_entry.get("extension", []) if "valueHumanName" in ext]),
            'id': merged_entry.get('id', ''),
            'active': merged_entry.get('active', ''),
            'practitioner': merged_entry.get('practitioner', ''),
            'organization': merged_entry.get('organization', ''),
            'specialty': str([spec for spec in merged_entry.get("specialty", []) if any(c["system"] == "https://mos.esante.gouv.fr/NOS/TRE_R42-DESCnonQualifiant/FHIR/TRE-R42-DESCnonQualifiant" for c in spec.get("coding", []))]),
            'identifier': str(merged_entry.get('identifier', '')),
            'name': merged_entry.get('name', ''),
            'qualification': str([{"code": {"coding": [c for c in qual["code"]["coding"] if c["system"] == "https://mos.esante.gouv.fr/NOS/TRE_R48-DiplomeEtatFrancais/FHIR/TRE-R48-DiplomeEtatFrancais"]}} for qual in merged_entry.get("qualification", []) if "code" in qual])
        }

        merged_data.append(filtered_entry)

def preprocess_cleaned_data(clean_df, rpps_data):
    for entry in clean_df:
        if isinstance(entry['specialty'], str):
            entry['specialty'] = json.loads(entry['specialty'].replace("'", '"'))
        entry['specialty'] = entry['specialty'][0] if entry['specialty'] else None

        if isinstance(entry['qualification'], str):
            entry['qualification'] = json.loads(entry['qualification'].replace("'", '"'))
        entry['qualification'] = entry['qualification'][0] if entry['qualification'] else None

        if isinstance(entry['identifier'], str):
            entry['identifier'] = json.loads(entry['identifier'].replace("'", '"'))
        entry['identifier'] = list(set(entry['identifier']))
        entry['identifier'] = entry['identifier'][0] if entry['identifier'] else None
        entry['identifier'] = int(entry['identifier']) if entry['identifier'] else None

        if entry['identifier'] is not None:
            entry['identifier'] = pd.to_numeric(entry['identifier'], errors='coerce')
            merged_row = rpps_data[rpps_data['Identification nationale PP'] == entry['identifier']]

            if not merged_row.empty:
                entry.update(merged_row.iloc[0].to_dict())

    return clean_df

clean_df = []
for row in merged_data:
    cleaned_row = clean_and_map_v3([row], specialty_mapping, diploma_mapping)
    clean_df.extend(cleaned_row)

clean_df = preprocess_cleaned_data(clean_df, rpps_data)

if clean_df:
    headers = list(clean_df[0].keys())

    with open('data/practitioner.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        writer.writeheader()
        for entry in clean_df:
            writer.writerow(entry)

    print("Cleaned data has been saved as practitioner.csv.")
else:
    print("No data to save.")
