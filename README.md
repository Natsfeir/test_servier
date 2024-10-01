### Exécution du code
Avec Python 3.11 :
- `export GOOGLE_APPLICATION_CREDENTIALS="/chemin/vers/votre/fichier/admin_demo_test_servier_sa.json"`
- `pip install -r requirements.txt`
- `python main_local.py`

### Contexte et consigne
Le but est de nettoyer des données avec Pandas pour produire un fichier JSON. J'ai simuler un cas typique d'ingénieur data sur GCP :
- Ingestion depuis un bucket
- Nettoyage en staging
- Génération d'un JSON

### Solution d'Intégration sur GCP
J'ai decoupé mon code main_local.py en 3 parties
1. **Ingestion** :
   - Charger les données depuis le bucket.
   - Nettoyage optionnel.
   - Ingestion dans BigQuery.
   - Transfert vers un bucket d'archivage.
   - **Architecture** : 
     - Classe mère avec fonctions communes, étendue pour GCP ou AWS.
     - Implémentations :
       - **Pandas** : Nettoyage en mémoire, intégrable avec Airflow.
       - **Alternative** : Traitement direct dans BigQuery sans dépendre de la RAM.

2. **Nettoyage** :
   - Charger les données depuis BigQuery.
   - Nettoyer avec Pandas.
   - Ré-ingérer dans BigQuery.

3. **Génération du JSON** :
   - Extraire et transformer les données depuis BigQuery pour obtenir le JSON.

### Adaptation pour gros volumes de données

- **Ingestion** : Utiliser une table temporaire BigQuery avec une bibliothèque comme `GCPIngestionLibrary`. Nettoyer avec SQL pour minimiser l'usage de la RAM.
  
- **Nettoyage** : Utiliser du SQL dans BigQuery pour éviter les limitations de RAM. Adopter une stratégie incrémentale pour mettre à jour les tables cibles.

### Pourquoi cette solution est scalable
- Intégration dans des DAGs Airflow, paramétrable pour divers cas d'usage.
- Exemple de nettoyage :
   ```python
   def custom_cleaning_function(df):
       df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
       df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce').dt.strftime('%d/%m/%Y')
       return df
   gcp_ingestion_pd.run(bucket_name, data_set_id, custom_cleaning_function)
   ```
- Sous-classes possibles pour des besoins spécifiques, comme `SearchDrugs(GCPCleaner)` dans `gcp_cleaning.py`.
#### Reponse a la question : Adapter sa solution pour en faire une Solution Idéale pour gros volume de données
- **Ingestion** :
  - Charger dans une table temporaire BigQuery a l'aide des bibliotheque bigquery a partir d'un bucket comme `GCPIngestionLibrary` du code pour_aller_plus_loin.py
  - Rajouter du nettoyage avec une requête SQL pour limiter l'utilisation de la RAM.
- **Cleaning** :
  - Nettoyer avec code SQL BigQuery : nous ne serons donc pas limiter par la Ram
  - Trouver une stratégie incrémentale pour mettre à jour la table cible sans la recréer.

- **Pourquoi ma solution independante et scalable avec Dag** :
   - Le code est conçu pour être intégré dans des DAGs Airflow, paramétrable pour différents cas d'usage.
  - comme dans mon code gcp_ingestion.py:
     ```python
     def custom_cleaning_function(df):
         df['id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
         df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y', errors='coerce').dt.strftime('%d/%m/%Y')
         return df
     gcp_ingestion_pd.run(bucket_name, data_set_id, custom_cleaning_function)
     ```
   - Lors des use_case trop specifique : Possibilité de créer des sous-classes pour des cas d'usage en héritant de la classe principale : comme ma class :
   `SearchDrugs(GCPCleaner) de gcp_cleaning.py`
