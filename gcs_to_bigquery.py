from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import numpy as np
import tempfile


bq_client = bigquery.Client()
table_name = "Spotify"
dataset_name = "Rio3631"
project_id = "myproject-389323"
bucket_name = 'my_altschool-bucket'


def spotify():

    bq_schema = [
        bigquery.SchemaField("song_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("artist_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("played_at", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("timestamp", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("popularity", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("album_or_single", "STRING", mode="NULLABLE")
        ]


    # Create the BigQuery table
    table_id = f"{project_id}.{dataset_name}.{table_name}"
    table = bigquery.Table(table_id, schema=bq_schema)
    table = bq_client.create_table(table)  # Make an API request.
    print( "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    # Initialize Google Cloud Storage client and bucket
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)


    with tempfile.NamedTemporaryFile("w") as tempdir:
        file_uri = tempdir.name
        file_name = '2023_06_21_to_2023_06_28.csv'

        # Extract the file from Google Cloud Storage
        blob = bucket.blob(file_name)
        blob.download_to_filename(file_uri)
        print("csv file successfully downloaded from Google Cloud Storage.")

        df = pd.read_csv(file_uri)
        print("csv file read into dataframe")


        # Set up a job configuration
        job_config = bigquery.LoadJobConfig(autodetect=False)

        # Submit the job
        job = bq_client.load_table_from_dataframe(df, table_id, job_config=job_config)  

        # Wait for the job to complete and then show the job results
        job.result()  
        
        # Read back the properties of the table
        table = bq_client.get_table(table_id)  
        print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
        print("JOB SUCCESSFUL")
spotify()