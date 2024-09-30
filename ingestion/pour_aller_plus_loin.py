from gcp_ingestion import GCPIngestion
import json
from pathlib import Path
import pandas as pd
import json
from pathlib import Path
from google.api_core.exceptions import NotFound, GoogleAPIError


### Fonction pour aller plus loin
class GCPIngestionLibrary(GCPIngestion):
    def __init__(self, project_id: str):
        super().__init__(project_id)

    def load(self, bucket_name: str, blob_path: str, dataset_id: str, schema_path: str = None):
        """
        Loads a file into the raw BigQuery table.

        Args:
            bucket_name (str): Name of the bucket containing the file to load.
            blob_path (str): Path of the blob inside the bucket.
            schema_path (str): Path of the schema file inside the bucket. Optional.
            dataset_id (str): Dataset ID in BigQuery where the table is located.
        Raises:
            FileNotFoundError: If the file or bucket does not exist.
            GoogleAPIError: If a GCP error occurs.
        """
        try:
            schema = None
            autodetect = True  # Initialize autodetect as True

            if schema_path:
                # Download the schema file if provided
                schema_bucket = self.storage_client.bucket(bucket_name)
                schema_blob = schema_bucket.blob(schema_path)
                schema_content = schema_blob.download_as_string().decode("utf-8")
                schema = json.loads(schema_content)
                logger.info(f"Schema loaded from {schema_path}")
                autodetect = False  # Set autodetect to False if schema is provided
            else:
                logger.info("No schema provided. Using schema autodetection.")

            # Define the link to the file to load
            link_bucket = f"gs://{bucket_name}/{blob_path}"
            extension = Path(blob_path).suffix.lower()

            # Configure the load job based on file extension and schema
            job_config = self._get_job_config(extension, schema)
            job_config.autodetect = autodetect  # Set autodetect in job_config

            table = Path(blob_path).stem  # Get the file name without extension

            # Define the destination in BigQuery
            destination = f"{self.project_id}.{dataset_id}.{table}"

            # Start the load job
            logger.info(f"Loading file {link_bucket} into {destination}")
            load_job = self.bigquery_client.load_table_from_uri(
                source_uris=[link_bucket], destination=destination, job_config=job_config
            )
            load_job.result()  # Wait for the job to complete
            logger.info(f"Loading completed for {blob_path}")

        except NotFound:
            logger.error(f"File or bucket not found: {bucket_name}/{blob_path}")
            raise FileNotFoundError(f"File or bucket not found: {bucket_name}/{blob_path}")
        except GoogleAPIError as e:
            logger.error(f"Google API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            raise

    def run(self, full_bucket_path: str, data_set_id: str, schema_path: str = None):
        """
        Executes the complete ingestion process: load, clean, and archive.

        Args:
            full_bucket_path (str): Full path of the file inside the bucket (e.g., "sandbox-nbrami-sfeir-test-facto/clinical_trials.csv").
            schema_path (str): Path of the schema file inside the bucket. Optional.
        """
        """
        Executes the complete ingestion process: load, clean, and archive.

        Args:
            full_bucket_path (str): Full path of the file inside the bucket (e.g., "sandbox-nbrami-sfeir-test-facto/clinical_trials.csv").
            schema_path (str): Path of the schema file inside the bucket. Optional.
        """
        try:
            # Split the bucket name and blob path
            full_bucket_path_list = full_bucket_path.split("/", 1)
            bucket_name = full_bucket_path_list[0]
            blob_path = full_bucket_path_list[1]

            archive_bucket_name = f"{bucket_name}-archive"

            # Load data into BigQuery
            self.load(bucket_name, blob_path, data_set_id, schema_path)

            # Archive the file after successful ingestion
            self._move_file(bucket_name, blob_path, archive_bucket_name, archive=True)

        except Exception as e:
            logger.error(f"Error during ingestion execution: {e}")
            # Move the file to the error bucket
            error_bucket_name = f"{bucket_name}-errors"
            try:
                self._move_file(bucket_name, blob_path, error_bucket_name, archive=False)
            except Exception as e:
                pass

