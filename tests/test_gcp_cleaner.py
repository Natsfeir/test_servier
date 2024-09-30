import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from google.cloud import bigquery
from cleaning.gcp_cleaning import GCPCleaner, SearchDrugs

class TestGCPCleaner(unittest.TestCase):

    @patch('cleaning.gcp_cleaning.bigquery.Client')
    def test_load_from_bigquery(self, mock_bigquery_client):
        mock_client_instance = mock_bigquery_client.return_value
        mock_query = mock_client_instance.query
        mock_query.return_value.to_dataframe.return_value = pd.DataFrame({
            'id': [1, 2],
            'name': ['Aspirin', 'Ibuprofen']
        })
        
        cleaner = GCPCleaner('test_project')
        df = cleaner.load_from_bigquery('test_table')

        self.assertEqual(len(df), 2)
        self.assertEqual(df['name'][0], 'Aspirin')

    @patch('cleaning.gcp_cleaning.to_gbq')
    def test_load_into_bigquery(self, mock_to_gbq):
        df = pd.DataFrame({
            'id': [1],
            'name': ['Aspirin']
        })
        
        cleaner = GCPCleaner('test_project')
        cleaner.load_into_bigquery(df, 'test_dataset.test_table')

        mock_to_gbq.assert_called_once_with(
            df, destination_table='test_dataset.test_table',
            project_id='test_project', if_exists='replace'
        )

    def test_clean_str_columns(self):
        df = pd.DataFrame({
            'name': [' Aspirin ', 'Ibuprofen ']
        })
        
        cleaned_df = GCPCleaner.clean_str_columns(df, ['name'])
        
        self.assertEqual(cleaned_df['name'][0], 'aspirin')
        self.assertEqual(cleaned_df['name'][1], 'ibuprofen')

class TestSearchDrugs(unittest.TestCase):

    @patch('cleaning.gcp_cleaning.GCPCleaner.load_from_bigquery')
    def test_search_drugs(self, mock_load_from_bigquery):
        mock_load_from_bigquery.side_effect = [
            pd.DataFrame({
                'scientific_title': ['Study with Aspirin'],
                'journal': ['Journal A'],
                'date': ['01-01-2020']
            }),
            pd.DataFrame({'drug': ['Aspirin']}),
            pd.DataFrame({
                'title': ['Aspirin helps'],
                'journal': ['Journal B'],
                'date': ['01-02-2020']
            })
        ]
        
        search = SearchDrugs('test_project')
        result = search.run()

        expected_result = [{'Aspirin': {('clinical', 'Journal A', '01-01-2020'), ('pubmed', 'Journal B', '01-02-2020')}}]
        self.assertEqual(result, expected_result)

if __name__ == '__main__':
    unittest.main()
