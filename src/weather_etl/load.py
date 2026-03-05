from dotenv import load_dotenv
import os
from google.cloud import bigquery
from google.api_core.exceptions import Conflict, GoogleAPIError
import time
import logging 

logger = logging.getLogger(__name__)

load_dotenv()
project_id = os.getenv('PROJECT_ID')

client = bigquery.Client(project=project_id)

def create_dataset(project_id: str,
                   dataset_id: str,
                   location: str = "US",
                   default_table_expiration_ms: int | None = None,
                   labels: dict | None = None,
                   friendly_name: str | None = None,
                   description: str | None = None,
                   access_entries: list | None = None,
                   retry_sleep: float = 1.0):
    """
    Create a BigQuery dataset if it doesn't already exist.
    - project_id: GCP project id
    - dataset_id: dataset id (no project prefix)
    - location: 'US', 'EU', 'southamerica-east1', etc.
    - default_table_expiration_ms: default table expiration in milliseconds (optional)
    - labels: dict of labels (optional)
    - access_entries: list of bigquery.AccessEntry objects or dicts to define access controls
    """

    full_dataset_id = f"{project_id}.{dataset_id}"

    dataset = bigquery.Dataset(full_dataset_id)
    dataset.location = location

    # --- SANDBOX SAFE EXPIRATION (max < 60 days) ---
    max_expiration_ms = 59 * 24 * 60 * 60 * 1000  # 59 days

    if default_table_expiration_ms is None:
        dataset.default_table_expiration_ms = max_expiration_ms
    else:
        dataset.default_table_expiration_ms = min(
            int(default_table_expiration_ms),
            max_expiration_ms
        )

    # Required in sandbox for partitioned tables
    dataset.default_partition_expiration_ms = max_expiration_ms
    # ------------------------------------------------


    if friendly_name:
        dataset.friendly_name = friendly_name
    if description:
        dataset.description = description
    if default_table_expiration_ms is not None:
        dataset.default_table_expiration_ms = int(default_table_expiration_ms)
    if labels:
        dataset.labels = labels
    if access_entries:
        # Access entries should be bigquery.AccessEntry objects or dicts:
        dataset.access_entries = access_entries

    # Idempotent create: attempt create, if exists then return existing dataset
    try:
        created = client.create_dataset(dataset, exists_ok=False)  # will raise Conflict if exists
        logger.info(f"Created dataset {created.full_dataset_id} in {created.location}")
        return created
    except Conflict:
        # dataset exists — fetch and return it
        return client.get_dataset(full_dataset_id)
    except GoogleAPIError as e:
        logger.error(f"API error while creating dataset: {e}. Retrying once after {retry_sleep}s...")
        time.sleep(retry_sleep)
        return client.get_dataset(full_dataset_id)

# creates a partitioned table in google bigquery if doesn't already exists
def create_partitioned_table(schema_path : str) -> str:
    ds = create_dataset(
        project_id=project_id,
        dataset_id="daily_weather_data",
        location="southamerica-east1",           # São Paulo region
        default_table_expiration_ms=30 * 24 * 3600 * 1000,  # 30 days in ms
        description="Dataset for storing daily weather data"
    )

    table_id = f'{project_id}.{ds.dataset_id}.weather_partitioned'
    schema = client.schema_from_json(schema_path)
    
    table = bigquery.Table(table_id,schema)
    table.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,
                                                        field="date",  # partition by the 'date' column
                                                        expiration_ms=59 * 24 * 60 * 60 * 1000,  # 59 days in ms
    )

    try:
        table = client.create_table(table)
        logger.info(f'table {table_id} created')
        return table_id
    # table already exists
    except Conflict:
        logger.info(f'{table_id} already exists')
        return table_id

# load the data into google bigquery table        
def load_data_to_bigquery(table_id : str ,data_path : str, schema_path : str) -> None:    
    with open(data_path,'rb') as file:
        schema = client.schema_from_json(schema_path)
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV,
                                            skip_leading_rows=1,
                                            schema=schema)
        job = client.load_table_from_file(file,
                                          table_id,
                                          job_config=job_config)

        logger.info(f"Started load job {job.job_id}")

        job.result()

        logger.info("Job finished")
        logger.info(f"Errors: {job.errors}")