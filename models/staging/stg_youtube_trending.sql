{{ config(materialized='view') }}


with source_data as (
    select * from {{ source('youtube_trending', 'video_basic_audience') }}
)

select
    video_id,
    creator_channel_id,
    start_date,
    country_code,
    viewer_percentage
from source_data
