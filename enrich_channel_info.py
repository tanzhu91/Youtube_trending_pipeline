import os
import pandas as pd
from tqdm import tqdm
from googleapiclient.discovery import build
from google.cloud import bigquery


YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)


PROJECT_ID = "upheld-momentum-463013-v7"
DATASET_ID = "dbt_tdereli"
TARGET_DATASET = "dbt_tdereli"
SOURCE_TABLE = "stg_youtube_trending"
DEST_TABLE = "channel_info_enriched" 



client = bigquery.Client()

query = f"""
    SELECT DISTINCT video_id
    FROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE}`
"""
video_ids = [row["video_id"] for row in client.query(query)]


missing_ids_all = []

def fetch_channel_info(batch_ids):
    try:
        response = youtube.videos().list(
            part="snippet",
            id=",".join(batch_ids)
        ).execute()

        returned_items = response.get("items", [])
        returned_ids = {item["id"] for item in returned_items}
        missing_ids = list(set(batch_ids) - returned_ids)

        item_lookup = {item["id"]: item for item in returned_items}
        result = []
        for vid in batch_ids:
            if vid in item_lookup:
                item = item_lookup[vid]
                result.append({
                    "video_id": vid,
                    "channel_id": item["snippet"]["channelId"],
                    "channel_title": item["snippet"]["channelTitle"],
                })
            else:
                result.append({
                    "video_id": vid,
                    "channel_id": None,
                    "channel_title": None,
                })

        return result, missing_ids

    except Exception as e:
        print(f"[ERROR] Failed fetching batch: {e}")
        return [], batch_ids 


channel_info = []
all_missing_ids = []


BATCH_SIZE = 50

for i in tqdm(range(0, len(video_ids), BATCH_SIZE), desc="Fetching channel info"):
    batch = video_ids[i:i + BATCH_SIZE]
    result, missing = fetch_channel_info(batch)
    channel_info.extend(result)
    all_missing_ids.extend(missing)






def fetch_channel_id_from_title(title):
    try:
        response = youtube.search().list(
            q=title,
            type="channel",
            part="snippet",
            maxResults=1
        ).execute()
        items = response.get("items", [])
        if items:
            return items[0]["id"]["channelId"]
    except Exception as e:
        print(f"[WARN] Could not get channel_id for '{title}': {e}")
    return None



title_cache = {}
for row in channel_info:
    if row["channel_id"] is None and row["channel_title"]:
        title = row["channel_title"]
        if title not in title_cache:
            title_cache[title] = fetch_channel_id_from_title(title)
        row["channel_id"] = title_cache[title]



df_fixed = pd.DataFrame(channel_info)


job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    schema=[
        bigquery.SchemaField("video_id", "STRING"),
        bigquery.SchemaField("channel_id", "STRING"),
        bigquery.SchemaField("channel_title", "STRING"),
    ]
)


table_ref = f"{PROJECT_ID}.{TARGET_DATASET}.{DEST_TABLE}"
load_job = client.load_table_from_dataframe(df_fixed, table_ref, job_config=job_config)
load_job.result()



