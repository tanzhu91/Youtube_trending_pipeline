from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from tqdm import tqdm
from googleapiclient.discovery import build
from google.cloud import bigquery


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


PROJECT_ID = "upheld-momentum-463013-v7"
DATASET_ID = "dbt_tdereli"
SOURCE_TABLE = "stg_youtube_trending"
DEST_TABLE = "channel_info_enriched"


with DAG(
    dag_id='youtube_enrich_channels',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
    description='Fetch channel_id from YouTube API and write to BigQuery'
) as dag:

    def enrich_and_upload():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/secrets/gcp-creds.json"
        youtube = build("youtube", "v3", developerKey=os.getenv("YOUTUBE_API_KEY"))
        client = bigquery.Client(project=PROJECT_ID)

   
        query = f"SELECT DISTINCT video_id FROM `{PROJECT_ID}.{DATASET_ID}.{SOURCE_TABLE}`"
        video_ids = [row["video_id"] for row in client.query(query)]
        print(f"Fetched {len(video_ids)} unique video IDs")

       
        def fetch_batch(batch):
            try:
                response = youtube.videos().list(part="snippet", id=",".join(batch)).execute()
                return [{
                    "video_id": item["id"],
                    "channel_id": item["snippet"]["channelId"],
                    "channel_title": item["snippet"]["channelTitle"]
                } for item in response.get("items", [])]
            except Exception as e:
                print(f"API error: {e}")
                return []

        enriched = []
        for i in tqdm(range(0, len(video_ids), 50)):
            enriched.extend(fetch_batch(video_ids[i:i+50]))

        df = pd.DataFrame(enriched)
        print(f"Total enriched: {len(df)}")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            schema=[
                bigquery.SchemaField("video_id", "STRING"),
                bigquery.SchemaField("channel_id", "STRING"),
                bigquery.SchemaField("channel_title", "STRING"),
            ]
        )

        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{DEST_TABLE}"
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Uploaded to {table_ref}")

    enrich_task = PythonOperator(
        task_id='enrich_and_upload_channel_info',
        python_callable=enrich_and_upload,
    )

    enrich_task
