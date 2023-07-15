import pandas as pd
import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from google.cloud import storage


bucket_name = 'my_altschool-bucket'
file_name = "2023_06_21_to_2023_06_28.csv"

storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)
# Extract the file from Google Cloud Storage
blob = bucket.blob(file_name)
blob.download_to_filename(file_name)

#read the dataset
df = pd.read_csv(file_name)


j = df.to_json(orient='index')
data = json.loads(j)



uri = "mongodb+srv://gospelmairo:G067tf0jXluG3Ppu@cluster0.zvl5oi1.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


db = client.get_database("altschool_database")

# Define the list of documents to insert
documents = []
for d, k in data.items():
    documents.append(k)

# Access the desired collection
collection = db.get_collection("Rio")

# Insert each document into the collection
inserted_ids = []
for document in documents:
    result = collection.insert_one(document)
    print(result.inserted_id)


