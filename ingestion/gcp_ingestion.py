import pandas as pd
import io
import json
import logging
from pathlib import Path

from google.cloud import storage, bigquery
from pandas_gbq import to_gbq
from google.api_core.exceptions import NotFound, GoogleAPIError

from ingestion.ingestion_abstract import Ingestion

# Configure the logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GCPIngestion(Ingestion):
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.storage_client = storage.Client(project=project_id)
        self.bigquery_client = bigquery.Client(project=project_id)

    def load(self):
        pass

    def run(self, full_bucket_path: str, schema_path: str = None):
        pass

    def _get_job_config(self, extension: str, schema: list = None) -> bigquery.LoadJobConfig:
        """Returns the load job configuration based on file extension."""
        if extension == ".csv":
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
            )
        elif extension == ".json":
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
        else:
            raise NotImplementedError(f"File extension {extension} is not supported")
        return job_config

    def _move_file(self, bucket_name: str, blob_path: str, target_bucket_name: str, archive: bool = True):
        """
        Moves the processed file to a target bucket (either archive or error bucket).

        Args:
            bucket_name (str): Name of the source bucket.
            blob_path (str): Path of the blob inside the source bucket.
            target_bucket_name (str): Name of the target bucket.
            archive (bool): If True, indicates the file is being archived, otherwise moved to error bucket.
        """
        try:
            # Get the source bucket and blob
            source_bucket = self.storage_client.bucket(bucket_name)
            source_blob = source_bucket.blob(blob_path)

            # Define the destination blob in the target bucket
            suffix = "_archive" if archive else "_error"
            target_bucket = self.storage_client.bucket(target_bucket_name)
            target_blob_path = f"{Path(blob_path).stem}{suffix}{Path(blob_path).suffix}"
            target_blob = target_bucket.blob(target_blob_path)

            # Copy the file to the target bucket
            source_bucket.copy_blob(source_blob, target_bucket, target_blob_path)
            logger.info(f"File moved successfully to {target_blob_path} in {target_bucket_name}")

            # Optionally, delete the original file from the source bucket
            #source_blob.delete()
            #logger.info(f"Original file {blob_path} deleted from source bucket")

        except NotFound as e:
            logger.error(f"Source or target bucket not found: {bucket_name} or {target_bucket_name}")
            raise FileNotFoundError(f"Source or target bucket not found: {bucket_name} or {target_bucket_name}") from e
        except GoogleAPIError as e:
            logger.error(f"Google API error during file move: {e}")
            raise RuntimeError(f"Google API error during file move: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during file move: {e}")
            raise RuntimeError(f"Unexpected error during file move: {e}") from e


class GCPIngestionPandas(GCPIngestion):
    def __init__(self, project_id: str):
        super().__init__(project_id)
        self.storage_client = storage.Client(project=self.project_id)

    def load_from_bucket(self, bucket_name: str, blob_path: str):
        """
        Loads a file into the raw BigQuery table using pandas-gbq.

        Args:
            full_bucket_path (str): Full path of the file inside the bucket (e.g., "sandbox-nbrami-sfeir-test-facto/clinical_trials.csv").
            dataset_id (str): Full table ID in BigQuery where the data should be loaded.

        Raises:
            FileNotFoundError: If the file or bucket does not exist.
            GoogleAPIError: If a GCP error occurs.
        """
        try:
            # Download the file from the GCS bucket into a Pandas DataFrame
            return self._download_blob_to_dataframe(bucket_name, blob_path)
        except Exception as e:
            logger.error(f"Error loading data into BigQuery with pandas-gbq: {e}")
            raise

    def clean_data(self, df: pd.DataFrame, clean_func) -> pd.DataFrame:
        """
        Cleans the DataFrame using a specified cleaning function.

        Args:
            df (pd.DataFrame): The DataFrame to be cleaned.
            clean_func (function): The cleaning function to apply.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        try:
            df = clean_func(df)
            logger.info("Data cleaned successfully.")
            return df
        except Exception as e:
            logger.error(f"Error cleaning data: {e}")
            raise

    def load_into_bigquery(self, df: pd.DataFrame, dataset_id: str, table_id: str):
        """
        Loads the cleaned data into a BigQuery table using pandas-gbq.

        Args:
            df (pd.DataFrame): The cleaned DataFrame.
            dataset_id (str): The dataset ID in BigQuery.
            table_id (str): The table ID in BigQuery where the data should be loaded.

        Raises:
            GoogleAPIError: If a GCP error occurs.
        """
        try:
            destination_table = f"{dataset_id}.{table_id}"
            to_gbq(df, destination_table=destination_table, project_id=self.project_id, if_exists="append")
            logger.info(f"Data loaded into BigQuery table {destination_table}")
        except Exception as e:
            logger.error(f"Error loading data into BigQuery table {dataset_id}.{table_id}: {e}")
            raise

    def _download_blob_to_dataframe(self, bucket_name: str, blob_path: str) -> pd.DataFrame:
        """
        Downloads a blob from GCS and reads it into a Pandas DataFrame.

        Args:
            bucket_name (str): The name of the GCS bucket.
            blob_path (str): The path to the file in the GCS bucket.

        Returns:
            pd.DataFrame: The data read from the blob into a Pandas DataFrame.
        """
        try:
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(blob_path)
            extension = Path(blob_path).suffix.lower()
            # Download the file content as a string
            content = blob.download_as_text()
            # Create a DataFrame from the content
            if extension == ".csv":
                df = pd.read_csv(io.StringIO(content))
            elif extension == ".json":
                content = self.clean_json_string(content)
                df = pd.read_json(io.StringIO(content))
            logger.info(f"Downloaded {blob_path} from bucket {bucket_name} and read into a DataFrame.")
            return df
        except Exception as e:
            logger.error(f"Error downloading blob {blob_path} from bucket {bucket_name}: {e}")
            raise

    def run(self, full_bucket_path: str, dataset_id: str, clean_func=None):
        """
        Executes the complete ingestion process using pandas-gbq.

        Args:
            full_bucket_path (str): Full path of the file inside the bucket (e.g., "bucket-name/folder/file.csv").
            dataset_id (str): The dataset ID in BigQuery where the data should be loaded.
            clean_func (function, optional): A cleaning function to apply to the DataFrame.

        Raises:
            FileNotFoundError: If the file or bucket does not exist.
            GoogleAPIError: If a GCP error occurs.
        """
        try:
            # Parse bucket path to get bucket name and blob path
            bucket_name, blob_path = full_bucket_path.split("/", 1)
            table_id = Path(blob_path).stem  # Extract table name from blob path

            # Download data from GCS into a DataFrame
            df = self.load_from_bucket(bucket_name, blob_path)

            # Clean data if a cleaning function is provided
            if clean_func:
                df = clean_func(df)

            # Load data into BigQuery
            self.load_into_bigquery(df, dataset_id, table_id)

            # Step 5: Archive or handle the file as needed (e.g., move to another bucket)
            archive_bucket_name = f"{bucket_name}-archive"
            self._move_file(bucket_name, blob_path, archive_bucket_name, archive=True)

        except Exception as e:
            logger.error(f"Error during ingestion execution: {e}")
            # Handle error case, move file to error bucket if necessary
            error_bucket_name = f"{bucket_name}-errors"
            try:
                self._move_file(bucket_name, blob_path, error_bucket_name, archive=False)
            except Exception as error:
                logger.error(f"Failed to move file to error bucket: {error}")


# Testing the code
if __name__ == "__main__":
    project_id = "sandbox-nbrami-sfeir"
    bucket_names = [
        "sandbox-nbrami-sfeir-test-facto/clinical_trials.csv",
        "sandbox-nbrami-sfeir-test-facto/drugs.csv",
        "sandbox-nbrami-sfeir-test-facto/pubmed.csv",
    ]
    schema_path = None
    data_set_id = "servier_test"
    gcp_ingestion_pd = GCPIngestionPandas(project_id)
    for bucket_name in bucket_names:
        gcp_ingestion_pd.run(bucket_name, data_set_id)
    bucket_name = "sandbox-nbrami-sfeir-test-facto/pubmed.json"

    def custom_cleaning_function(df):
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")
        df["date"] = pd.to_datetime(df["date"], format="%d/%m/%Y", errors="coerce").dt.strftime("%d/%m/%Y")
        return df

    gcp_ingestion_pd.run(bucket_name, data_set_id, custom_cleaning_function)
