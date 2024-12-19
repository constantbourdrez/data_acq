import pandas as pd

test  = pd.read_csv('data/final_data.csv')
test[['vision_generale_all', 'vision_profession_territoire',
      'vision_honoraires_remunerations_niveau_2',
      'vision_honoraires_actes_niveau_2',
      'vision_honoraires_actescliniques_niveau_3',
      'vision_honoraires_actestechniques_niveau_3',  'vision_generale_prescriptions']] = test[['vision_generale_all', 'vision_profession_territoire',
                                                             'vision_honoraires_remunerations_niveau_2',
                                                             'vision_honoraires_actes_niveau_2',
                                                             'vision_honoraires_actescliniques_niveau_3',
                                                             'vision_honoraires_actestechniques_niveau_3',  'vision_generale_prescriptions']].replace({'oui': 1, 'non': 0})
test["montant_moyen_prescription"] = test["montant_moyen_prescription"].replace('NS', 0)

test["montant_moyen_prescription"] = test["montant_moyen_prescription"].astype(int)

grouped_ameli_data = test.groupby(['profession_sante', 'annee', 'departement']).agg({
    'honoraires_ordre_niv_1': 'sum',
    'honoraires_ordre_niv_2': 'sum',
    'honoraires_ordre_niv_3': 'sum',
    'montant_honoraires': 'sum',
    'montant_honoraires_moyens': 'mean',
    'vision_generale_all': 'mean',
    'vision_profession_territoire': 'mean',
    'vision_honoraires_remunerations_niveau_2': 'mean',
    'vision_honoraires_actes_niveau_2': 'mean',
    'vision_honoraires_actescliniques_niveau_3': 'mean',
    'vision_honoraires_actestechniques_niveau_3': 'mean',
    'poste_prescription': 'sum',
    'libelle_poste_prescription': 'first',
    'montant_total_prescription': 'sum',
    'montant_moyen_prescription': 'mean',
    'vision_generale_prescriptions': 'mean',
    'montant_total_prescription_integer': 'sum',
    'montant_moyen_prescription_integer': 'mean',
    'effectif': 'sum',
    'honoraires': 'sum',
    'prescriptions': 'sum',

}).reset_index()

numeric_columns = grouped_ameli_data.select_dtypes(include=['number']).columns
categorical_columns = grouped_ameli_data.select_dtypes(include=['object']).columns


grouped_ameli_data = grouped_ameli_data[numeric_columns.tolist() + categorical_columns.tolist()]

grouped_ameli_data = grouped_ameli_data[['annee', 'honoraires_ordre_niv_1', 'honoraires_ordre_niv_2',
       'honoraires_ordre_niv_3', 'montant_honoraires',
       'montant_honoraires_moyens', 'vision_generale_all',
       'vision_profession_territoire',
       'vision_honoraires_remunerations_niveau_2',
       'vision_honoraires_actes_niveau_2',
       'vision_honoraires_actescliniques_niveau_3',
       'vision_honoraires_actestechniques_niveau_3',
       'montant_moyen_prescription', 'vision_generale_prescriptions',
       'montant_total_prescription_integer',
       'montant_moyen_prescription_integer', 'effectif', 'honoraires',
       'prescriptions', 'profession_sante', 'departement', 'libelle_poste_prescription']]

grouped_ameli_data.to_csv('data/ameli_preprocessed.csv', index=False)
