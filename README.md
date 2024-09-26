### Solution d'Intégration sur GCP

1. **Cloud Storage** :
   - Utilisation de trois buckets : un pour les fichiers, un pour les archives, et un pour les erreurs.

2. **Pub/Sub** :
   - Notifie les événements de traitement.

3. **Code Python d'Ingestion** :
   - Étapes :
     - Charger les données depuis le bucket.
     - Nettoyage optionnel.
     - Ingestion dans BigQuery.
   - Architecture :
     - Classe mère avec des fonctions communes, étendue par des classes spécifiques pour GCP ou AWS.
   - Deux Implémentations :
     - **Pandas** : Nettoyage en mémoire, intégration possible avec Airflow.
     - **BigQuery** : Traitement direct dans BigQuery, sans dépendance à la RAM.

4. **Code Python de Nettoyage** :
   - Étapes :
     - Charger les données brutes depuis BigQuery.
     - Nettoyer avec Pandas.
     - Ingestion dans BigQuery.


#### Reponse a la question : Adapeter sa solution pour en faire une Solution Idéale pour gros volume de données
- **Ingestion** :
  - Charger dans une table temporaire BigQuery a l'aide des bibliotheque bigquery a partir d'un bucket comme `GCPIngestionLibrary`
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



