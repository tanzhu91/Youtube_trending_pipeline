from googleapiclient.discovery import build
import pandas as pd
import isodate
from datetime import datetime
from google.cloud import bigquery
import os




api_key = os.getenv("YOUTUBE_API_KEY")

youtube = build("youtube", "v3", developerKey=api_key)


region_codes = [
    'US', 'CA', 'GB', 'AU', 'IN', 'JP', 'KR', 'BR', 'MX', 'FR', 'DE', 'RU', 'IT', 'ES',
    'AR', 'CO', 'CL', 'NL', 'TR', 'SA', 'AE', 'EG', 'ID', 'MY', 'TH', 'VN', 'SG', 'NG',
    'KE', 'ZA', 'PK', 'BD', 'UA', 'PL', 'SE', 'CH', 'BE', 'NO', 'DK', 'FI', 'IE', 'NZ',
    'PH', 'HK', 'TW', 'IL', 'RO', 'HU', 'CZ', 'GR', 'PT', 'SK', 'AT'
]


video_data = []

for region in region_codes:
    response = youtube.videos().list(
        part="snippet,contentDetails,statistics",
        chart="mostPopular",
        regionCode=region,
        maxResults=50
    ).execute()

    for item in response['items']:
        snippet = item['snippet']
        stats = item.get('statistics', {})
        content = item['contentDetails']
        
        video_data.append({
            'video_id': item['id'],
            'title': snippet.get('title'),
            'description': snippet.get('description'),
            'channel_title': snippet.get('channelTitle'),
            'published_at': datetime.strptime(snippet.get('publishedAt'), "%Y-%m-%dT%H:%M:%SZ"),
            'category_id' : snippet.get('categoryId'),
            'default_language' : snippet.get('defaultLanguage'),
            'tags': ', '.join(snippet.get('tags', [])),
            'duration_seconds': isodate.parse_duration(content['duration']).total_seconds(),
            'view_count': int(stats.get('viewCount', 0)),
            'like_count': int(stats.get('likeCount', 0)),
            'comment_count': int(stats.get('commentCount', 0)),
        })




unique_data = {video['video_id']: video for video in video_data}
video_data = list(unique_data.values())



load_date = datetime.utcnow()
for video in video_data:
    video["load_date"] = load_date


df = pd.DataFrame(video_data)





client = bigquery.Client()


dataset_id = 'dbt_tdereli' 
table_id = 'youtube_trending_videos'

table_ref = client.dataset(dataset_id).table(table_id)


schema = [
    bigquery.SchemaField("load_date", "TIMESTAMP"),
    bigquery.SchemaField("video_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("channel_title", "STRING"),
    bigquery.SchemaField("published_at", "TIMESTAMP"),
    bigquery.SchemaField("category_id", "STRING"),
    bigquery.SchemaField("default_language", "STRING"),
    bigquery.SchemaField("tags", "STRING"),
    bigquery.SchemaField("duration_seconds", "FLOAT"),
    bigquery.SchemaField("view_count", "INTEGER"),
    bigquery.SchemaField("like_count", "INTEGER"),
    bigquery.SchemaField("comment_count", "INTEGER"),
]


job_config = bigquery.LoadJobConfig(
    schema=schema,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
)


job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
job.result()