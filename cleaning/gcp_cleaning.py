import pandas as pd
from google.cloud import bigquery
from pandas_gbq import to_gbq

class GCPCleaner:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.bigquery_client = bigquery.Client(project=project_id)
    
    def load_from_bigquery(self, source_table: str) -> pd.DataFrame:
        query = f"SELECT * FROM `{source_table}`"
        df = self.bigquery_client.query(query).to_dataframe()
        return df

    def clean_data(self, df: pd.DataFrame, clean_func) -> pd.DataFrame:
        df = clean_func(df)
        return df

    def load_into_bigquery(self, df: pd.DataFrame, destination_table: str):
        to_gbq(df, destination_table=destination_table, project_id=self.project_id, if_exists='replace')

    def run(self, source_table: str, destination_table: str, clean_func=None):
        df = self.load_from_bigquery(source_table)
        if clean_func:
            df = self.clean_data(df, clean_func)
        self.load_into_bigquery(df, destination_table)

    @classmethod
    def clean_str(cls, df: pd.DataFrame, name_column: str):
        df[name_column] = df[name_column].str.lower().str.strip()
        return df

    @classmethod
    def clean_str_columns(cls, df: pd.DataFrame, name_columns: list):
        for name_column in name_columns:
            df = cls.clean_str(df, name_column)
        return df

    @classmethod
    def convert_mixed_dates(cls, date: str) -> str:
        try:
            return pd.to_datetime(date, dayfirst=True).strftime('%m-%d-%Y')
        except ValueError:
            return pd.to_datetime(date).strftime('%m-%d-%Y')

    @classmethod
    def convert_mixed_dates_column(cls, df: pd.DataFrame, date_column: str) -> pd.DataFrame:
        df[date_column] = df[date_column].apply(lambda x: cls.convert_mixed_dates(x))
        return df



class SearchDrugs(GCPCleaner):
    def run(self):
        clinical_trials = self.load_from_bigquery("servier_test_staging.clinical_trials")
        drugs = self.load_from_bigquery("servier_test_staging.drugs")
        pubmed = self.load_from_bigquery("servier_test_staging.pubmed")

        drugn_json = []
        for drug in drugs.drug:
            clinical_trials_drug = clinical_trials[clinical_trials.scientific_title.str.contains(drug)]
            json_clinical = set(zip(["clinical" for _ in range(len(clinical_trials_drug))], clinical_trials_drug['journal'], clinical_trials_drug['date']))
            pubmed_drug = pubmed[pubmed.title.str.contains(drug)]
            json_pubmed = set(zip(["pubmed" for _ in range(len(pubmed_drug))], pubmed_drug['journal'], pubmed_drug['date']))
            json_clinical.update(json_pubmed)
            drugn_json.append({drug : json_clinical})
        # self.load_into_bigquery(drugn_json, 'servier_test_staging.drug_json')
        return drugn_json

if __name__ == '__main__':
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
        source_table = f"servier_test.{table_id}"
        destination_table = f"servier_test_staging.{table_id}"
        gcp_cleaner = GCPCleaner(project_id)
        gcp_cleaner.run(source_table, destination_table, clean_func)
    search = SearchDrugs(project_id)
    
    #J'ai formater le json en ayant des valeurs direct
    #Pour faciliter son utilisation on pourra le formater de la maniere:
    #[{"drug":valeur_drug, "journals":[{"name_jounal":valeur_name, "date":date},{"name_jounal":valeur_name, "date":date} ...]}...]
    drug_jsons = search.run()
    #bonus:
    journals_dict = {}
    def add(key):
        if key in journals_dict:
            journals_dict[key] += 1
        else:
            journals_dict[key] = 1
    for drug_json in drug_jsons:
        for name_drug, publish_journals_date in drug_json.items():
            publish_journals_date_clean = {journal for _, journal, _ in publish_journals_date}
            for journal in publish_journals_date_clean:
                add(journal)
    print(" le nom du journal qui mentionne le plus de médicaments différents:",max(journals_dict, key=journals_dict.get))
    # bonnus 2
    def find_medicaments_by_journal(journal):
        medicaments = []
        for drug_json in drug_jsons:
            for name_drug, publish_journals_date in drug_json.items():
                publish_journals_date_clean = {journal for source, journal, _ in publish_journals_date if source == 'pubmed'}
                if journal in publish_journals_date_clean:
                    medicaments.append(name_drug)
        return medicaments
    medicament_donne = "diphenhydramine"
    medicaments = []
    for drug_json in drug_jsons:
        for name_drug, publish_journals_date in drug_json.items():
            if name_drug == medicament_donne:
                publish_journals_date_clean = {journal for source, journal, _ in publish_journals_date if source == 'pubmed'}
                for journal in publish_journals_date_clean:
                    medicaments.extend(find_medicaments_by_journal(journal))

    print(f" l’ensemble des médicaments mentionnés par les mêmes journaux référencés de {medicament_donne} est ",set(medicaments))
