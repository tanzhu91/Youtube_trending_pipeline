{{ config(materialized='view') }}


with source_data as (
    select * from `upheld-momentum-463013-v7.youtube_dataset.trending_videos`
)

select
    video_id,
    title,
    channel_title,
    cast(view_count as int64) as view_count,
    cast(like_count as int64) as like_count,
    cast(comment_count as int64) as comment_count,
    published_at,
    duration_seconds
from source_data
