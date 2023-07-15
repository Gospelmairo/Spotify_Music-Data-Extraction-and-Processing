from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import tempfile
import json

# Set up Google Cloud project and bucket details
bucket_name = 'my_altschool-bucket'
dataset_name = 'Rio3631'
project_id = 'myproject-389323'
table_name = 'spotify_json'

bq_client = bigquery.Client()

def create_table():
    """
    Creates a BigQuery table with the defined schema.
    """
    # Define the table schema
    bq_schema = [
        bigquery.SchemaField("song_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("artist_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("played_at", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("popularity", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("album_or_single", "STRING", mode="NULLABLE")
        ]


    # Create the BigQuery table
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    table = bigquery.Table(table_id, schema=bq_schema)
    table = bq_client.create_table(table)  # Make an API request.
    print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

def json_to_bigquery():
    """
    Downloads a CSV file from Google Cloud Storage, converts it to JSON,
    and loads the data into a BigQuery table.
    """

    # Extract the file from Google Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    with tempfile.NamedTemporaryFile('w') as tempdir:
        file_uri = tempdir.name
        file_name = '2023_06_21_to_2023_06_28.csv'

        # Extract the file from Google Cloud Storage
        blob = bucket.blob(file_name)
        blob.download_to_filename(file_uri)
        print('File successfully downloaded from Google Cloud Storage.')

        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(file_uri)
        print('CSV file read into dataframe')

        # Convert DataFrame to JSON string
        json_string = df.to_json(orient='records')

        # Convert the JSON string to a list of dictionaries
        json_data = json.loads(json_string)

        # Create a job configuration
        job_config = bigquery.LoadJobConfig(autodetect=False)

        # Define the table reference
        # table_ref = bq_client.dataset(dataset_name).table(table_name)
        table_id = f"{project_id}.{dataset_name}.{table_name}"

        # Load JSON data from the list of dictionaries
        load_job = bq_client.load_table_from_json(json_data, table_id, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        print(f"Data loaded successfully.")

create_table()
json_to_bigquery()