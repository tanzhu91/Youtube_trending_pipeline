{{ config(materialized='view') }}

with base as (
    select * from {{ ref('stg_youtube_trending') }}
),

metrics as (
    select
        published_at,
        FORMAT_TIMESTAMP('%H:%M', published_at) AS published_hour,
        load_date,
        FORMAT_TIMESTAMP('%H:%M', load_date) AS load_hour,
        video_id,
        title,
        channel_title,
        category_name,
        duration_seconds,
        duration_minutes,
        duration_hours,
        view_count,
        like_count,
        comment_count,
        safe_divide(like_count, view_count) AS like_rate,
        safe_divide(comment_count, view_count) AS comment_rate,
        duration_minutes > 10 AS is_long_video,
        view_count > 1000000 AS is_viral

    from base
)

select * from metrics
