{{ config(materialized='view') }}

with base as (
    select * from {{ ref('stg_youtube_trending') }}
),

metrics as (
    select
        video_id,
        title,
        channel_title,
        published_at,
        load_date,
        category_name,
        duration_seconds,
        duration_minutes,
        duration_hours,
        view_count,
        like_count,
        comment_count,
        safe_divide(like_count, view_count) as like_rate,
        safe_divide(comment_count, view_count) as comment_rate,
        duration_minutes > 10 as is_long_video,
        view_count > 1_000_000 as is_viral

    from base
)

select * from metrics
