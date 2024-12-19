import requests
from bs4 import BeautifulSoup
import json

base_url_1 = "https://mos.esante.gouv.fr/NOS/TRE_R42-DESCnonQualifiant/FHIR/TRE-R42-DESCnonQualifiant"
base_url_2 = "https://mos.esante.gouv.fr/NOS/TRE_R48-DiplomeEtatFrancais/FHIR/TRE-R48-DiplomeEtatFrancais/TRE_R48-DiplomeEtatFrancais-FHIR.json"

def scrape_index_for_json_links(base_url):
    response = requests.get(base_url)
    if response.status_code != 200:
        print(f"Error fetching index page: {response.status_code}")
        return []
    soup = BeautifulSoup(response.content, "html.parser")
    links = soup.find_all("a")
    json_links = []
    for link in links:
        href = link.get("href")
        if href and href.endswith(".json"):
            json_links.append(base_url + "/" + href)
    return json_links

def fetch_code_meaning_from_json(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            code_meanings = []
            for concept in data.get("concept", []):
                code_meanings.append({
                    "code": concept['code'],
                    "meaning": concept.get('display', 'No display found')
                })
            return code_meanings
        else:
            print(f"Error fetching data from {url}: {response.status_code}")
            return []
    except Exception as e:
        print(f"Error fetching data from {url}: {e}")
        return []

# Fetch links from base URL 1
json_links_1 = scrape_index_for_json_links(base_url_1)

# Fetch code meanings for base URL 1
all_code_meanings_1 = []
for link in json_links_1:
    code_meanings = fetch_code_meaning_from_json(link)
    all_code_meanings_1.extend(code_meanings)

# Fetch code meanings from base URL 2 (direct JSON URL)
all_code_meanings_2 = fetch_code_meaning_from_json(base_url_2)

# Combine both datasets into one final structure
final_data = {
    "TRE_R42_DESCnonQualifiant": all_code_meanings_1,
    "TRE_R48_DiplomeEtatFrancais": all_code_meanings_2
}

# Save the combined data to a JSON file
output_filename = "data/profession_diploma.json"
with open(output_filename, 'w', encoding='utf-8') as json_file:
    json.dump(final_data, json_file, ensure_ascii=False, indent=4)

print(f"Code meanings have been saved to {output_filename}")
