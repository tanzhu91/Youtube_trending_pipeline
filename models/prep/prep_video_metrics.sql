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
        EXTRACT(YEAR  FROM published_at) as published_year,
        EXTRACT(MONTH FROM published_at) as published_month,
        EXTRACT(DAY   FROM published_at) as published_day,
        FORMAT_TIMESTAMP('%H:%M', published_at) AS published_hour,
        load_date,
        EXTRACT(YEAR  FROM load_date) as load_year,
        EXTRACT(MONTH FROM load_date) as load_month,
        EXTRACT(DAY   FROM load_date) as load_day,
        FORMAT_TIMESTAMP('%H:%M', load_date) AS load_hour,
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
