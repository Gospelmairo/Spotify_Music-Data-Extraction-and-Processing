import json
import requests
import pandas as pd
from secret_file import spotify_user_id
from datetime import datetime, date, time, timedelta
from refresh_token import Refresh
from google.cloud import storage

class SaveSongs:
    def __init__(self):
        self.user_id = spotify_user_id
        self.spotify_token = ""
        self.tracks = ""


    def get_recently_played(self):
            # Convert time to Unix timestamp in miliseconds
            today = datetime.now()
            past_7_days = today - timedelta(days=8)
            print(int(past_7_days.timestamp()))
            past_7_days_unix_timestamp = int(past_7_days.timestamp()) * 1000
            # Download all songs you've listened to "after yesterday", which means in the last 24 hours
            endpoint = "https://api.spotify.com/v1/me/player/recently-played?after={time}".format(
                time=past_7_days_unix_timestamp)
            headers={"Content-Type": "application/json",
                                             "Authorization": "Bearer {}".format(self.spotify_token)}
             # --header 'Authorization: Bearer undefined...undefined'
            r = requests.get(endpoint, headers=headers, params={"limit": 50})
            if r.status_code not in range(200, 299):
                return {}
            print(r.json())
            return r.json()


    def call_refresh(self):

        print("Refreshing token")

        refreshCaller = Refresh()

        self.spotify_token = refreshCaller.refresh()

        #self.get_recently_played()

a = SaveSongs()
a.call_refresh()

data = a.get_recently_played()

song_names = []
artist_names = []
played_at_list = []
timestmps = []
popularity = []
album_or_single = []

# Extracting only the relevant bits of data from the json object
for song in data["items"]:
    song_names.append(song["track"]["name"])
    artist_names.append(song["track"]["album"]["artists"][0]["name"])
    played_at_list.append(song["played_at"])
    timestmps.append(song["played_at"][0:10])
    popularity.append(song["track"]["popularity"])
    album_or_single.append(song["track"]["album"]["album_type"])

# Prepare a dictionary in order to turn it into a pandas dataframe below
song_dict = {
    "song_name": song_names,
    "artist_name": artist_names,
    "played_at": played_at_list,
    "timestamp": timestmps,
    "popularity": popularity,
    "album_or_single": album_or_single
}

song_df = pd.DataFrame(song_dict, columns=[
     "song_name", "artist_name", "played_at", "timestamp", "popularity", "album_or_single"])

print(song_df)

song_df.to_csv("2023_06_21_to_2023_06_28.csv", index=False)
bucket_name = "my_altschool-bucket"
file_path = "2023_06_21_to_2023_06_28.csv"

#upload the files to GCS
storage_client = storage.Client()
bucket = storage_client.bucket(bucket_name)

blob = bucket.blob(file_path)
blob.upload_from_filename(file_path)
print(f"file successfully uploaded to Google Cloud Storage.")
print("Success!")