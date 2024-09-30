from ingestion.gcp_ingestion import GCPIngestionPandas
from cleaning.gcp_cleaning import GCPCleaner, SearchDrugs
import pandas as pd
from google.cloud import bigquery
import json 

def clear_table(project_id, data_set_id):
    print("clear tables")
    table_names = [
        "clinical_trials",
        "drugs",
        "pubmed",
    ]

    client = bigquery.Client(project=project_id)
    
    for table_name in table_names:
        query = f"DELETE FROM `{project_id}.{data_set_id}.{table_name}` WHERE TRUE"
        client.query(query).result()

if __name__ == '__main__':
    project_id = 'sandbox-nbrami-sfeir'
    data_set_id = "servier_test"
    # Avant de commencer nous devons vider les tables déjà presente dans le projet
    # pour pouvoir executer plusieur fois le code 
    clear_table(project_id, data_set_id)


    ####### 1. **Code Python d'Ingestion** :#########
    # En utilisant le cloud :
    bucket_names = [
        "sandbox-nbrami-sfeir-test-facto/clinical_trials.csv",
        "sandbox-nbrami-sfeir-test-facto/drugs.csv",
        "sandbox-nbrami-sfeir-test-facto/pubmed.csv",
        ]
    gcp_ingestion_pd = GCPIngestionPandas(project_id)
    for bucket_name in bucket_names:
        print("ingestion", bucket_name)
        gcp_ingestion_pd.run(bucket_name, data_set_id)
        print('\n\n\n')
    bucket_name = "sandbox-nbrami-sfeir-test-facto/pubmed.json"
    def custom_cleaning_function(df):
        df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce').dt.strftime('%d/%m/%Y')
        return df
    print("ingestion", bucket_name)
    gcp_ingestion_pd.run(bucket_name, data_set_id, custom_cleaning_function)
    print('\n\n\n')

    #######2. **Code Python de Nettoyage** :
    project_id = 'sandbox-nbrami-sfeir'
    def clean_clinical_trials(df):
        df = GCPCleaner.clean_str_columns(df, ["scientific_title", "journal"])
        df = GCPCleaner.convert_mixed_dates_column(df, 'date')
        return df

    def clean_drugs(df):
        df = GCPCleaner.clean_str_columns(df, ["drug"])
        return df

    def clean_pubmed(df):
        df = GCPCleaner.clean_str_columns(df, ["title", "journal"])
        df = GCPCleaner.convert_mixed_dates_column(df, 'date')
        return df

    table_cleaning_funcs = {
        "clinical_trials": clean_clinical_trials,
        "drugs": clean_drugs,
        "pubmed": clean_pubmed
    }

    for table_id, clean_func in table_cleaning_funcs.items():
        print("clean of", table_id)
        source_table = f"servier_test.{table_id}"
        destination_table = f"servier_test_staging.{table_id}"
        gcp_cleaner = GCPCleaner(project_id)
        gcp_cleaner.run(source_table, destination_table, clean_func)
        print('\n\n\n')

    search = SearchDrugs(project_id)
    
    ####### 3. **Obtention du Json** :
    #J'ai formater le json en ayant des valeurs direct
    drug_data = search.run()

    #Pour faciliter son utilisation on pourra le formater de la maniere:
    #[{"drug":valeur_drug, "journals":[{"name_jounal":valeur_name, "date":date},{"name_jounal":valeur_name, "date":date} ...]}...]
    drug_json = []
    for entry in drug_data:
        for drug, journal_entries in entry.items():
            journals = [{"name_journal": journal, "date": date} for _, journal, date in journal_entries]
            drug_json.append({"drug": drug, "journals": journals})
    
    with open("./data/drug_json_result.json", "w") as json_file:
        json.dump(drug_json, json_file)
    print("drug_json:", drug_json)
    print('\n\n\n')
    print('\n\n\n')

    #bonus:
    journals_dict = {}
    def add(key):
        if key in journals_dict:
            journals_dict[key] += 1
        else:
            journals_dict[key] = 1
    for drug_json in drug_data:
        for name_drug, publish_journals_date in drug_json.items():
            publish_journals_date_clean = {journal for _, journal, _ in publish_journals_date}
            for journal in publish_journals_date_clean:
                add(journal)
    print(" le nom du journal qui mentionne le plus de médicaments différents:",max(journals_dict, key=journals_dict.get))
    print('\n\n\n')
    # bonnus 2
    def find_medicaments_by_journal(journal):
        medicaments = []
        for drug_json in drug_data:
            for name_drug, publish_journals_date in drug_json.items():
                publish_journals_date_clean = {journal for source, journal, _ in publish_journals_date if source == 'pubmed'}
                if journal in publish_journals_date_clean:
                    medicaments.append(name_drug)
        return medicaments
    medicament_donne = "diphenhydramine"
    medicaments = []
    for drug_json in drug_data:
        for name_drug, publish_journals_date in drug_json.items():
            if name_drug == medicament_donne:
                publish_journals_date_clean = {journal for source, journal, _ in publish_journals_date if source == 'pubmed'}
                for journal in publish_journals_date_clean:
                    medicaments.extend(find_medicaments_by_journal(journal))

    print(f" l’ensemble des médicaments mentionnés par les mêmes journaux référencés de {medicament_donne} est ",set(medicaments))
    print('\n\n\n')
