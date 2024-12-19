from sentence_transformers import SentenceTransformer, util
import csv
import numpy as np
import torch

# Step 1: Load profession_sante and e_sante profession data
profession_sante = [
    'Médecins généralistes (hors médecins à expertise particulière - MEP)',
    'Médecins généralistes à expertise particulière (MEP)',
    'Médecins nucléaires', 'Médecins pathologistes', 'Dermatologues',
    'Endocrinologues', 'Ensemble des médecins',
    'Ensemble des médecins spécialistes (hors généralistes)',
    'Gynécologues médicaux et obstétriciens',
    'Hépato-gastro-entérologues', 'Psychiatres', 'Pédiatres',
    'Radiologues', 'Radiothérapeutes', 'Autres médecins',
    'Cardiologues', 'Chirurgiens', 'Néphrologues', 'Ophtalmologues',
    'Oto-rhino-laryngologistes', 'Pneumologues',
    'Médecins vasculaires', 'Neurologues', 'Allergologues',
    'Anesthésistes-réanimateurs', 'Rhumatologues', 'Stomatologues'
]


e_sante_professions = []
with open('data/practitioner.csv') as e_file:
    reader = csv.DictReader(e_file, delimiter=',')
    for row in reader:
        if 'qualification' in row:
            e_sante_professions.append(row['Libellé profession'])


print("Mapping professions with Sentence Transformers...")
model = SentenceTransformer('paraphrase-xlm-r-multilingual-v1')# Multilingual lightweight model

# Encode professions
profession_sante_embeddings = model.encode(profession_sante, convert_to_tensor=True)
e_sante_professions_embeddings = model.encode(list(set(e_sante_professions)), convert_to_tensor=True)

# Compute cosine similarity
similarities = util.pytorch_cos_sim(e_sante_professions_embeddings, profession_sante_embeddings)

# Extract best matches
profession_mapping = {}
for i, e_profession in enumerate(set(e_sante_professions)):
    best_match_idx = torch.argmax(similarities[i]).item()
    best_match = profession_sante[best_match_idx]
    similarity_score = similarities[i][best_match_idx].item()
    profession_mapping[e_profession] = (best_match, similarity_score)

# Step 4: Print the mappings and save to file
with open('profession_mapping.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['e_sante_profession', 'profession_sante', 'similarity_score'])
    for e_profession, (match, score) in profession_mapping.items():
        writer.writerow([e_profession, match, score])

print("Profession mapping complete. Saved to 'profession_mapping.csv'.")
