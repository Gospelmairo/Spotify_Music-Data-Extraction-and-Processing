# Spotify_Music-Data-Extraction-and-Processing
This repository contains Python scripts to pull recently listened music data from your Spotify account and store it in various formats such as CSV and JSON. The data can be pushed to Google Cloud Storage (GCS), BigQuery, and a local MongoDB instance.

## Prerequisites
Before running the scripts, make sure you have the following:

1. Python: Ensure you have Python installed on your machine. You can download and install Python from the official website: https://www.python.org/downloads/
2. Spotify API Credentials: Obtain your Spotify API credentials by creating a Spotify Developer account and registering your application. You will need the CLIENT_ID, CLIENT_SECRET, and REDIRECT_URI to authenticate and access your Spotify data.
3. Google Cloud Platform (GCP) Account: Create a GCP account and set up a project with access to GCS and BigQuery. You will need your GCP project ID, GCS bucket name, and BigQuery dataset and table information.
4. MongoDB: Install MongoDB on your local machine. You can download and install MongoDB Community Edition from the official website: https://www.mongodb.com/try/download/community


## The required programming languages for this project are:

1.Python: The main language for writing the scripts and interacting with the Spotify API, Google Cloud Platform services (GCS and BigQuery), and MongoDB.

Additionally, you may need some knowledge of the following:
1. SQL: Required for working with BigQuery as you will need to write SQL queries to load data into BigQuery tables.
2. JSON: You'll be working with JSON data, so having a basic understanding of JSON syntax will be helpful for processing and transforming the data.

## Libraries
1. Requests
2. json
3. pandas
4. datetime
5. google-cloud-storage
6. pymongo






