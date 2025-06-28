{{ config(materialized='table') }}

with base as (
    select * from {{ ref('prep_video_metrics') }}
),

daily_stats as (
    select
        title,
        channel_title,
        category_name,
        DATE(load_date) as load_date,
        load_hour,
        DATE(published_at) as published_at,
        published_hour,
        view_count,
        like_count,
        comment_count
    from base
    group by video_id, title, channel_title, category_name,  DATE(load_date) , load_hour, DATE(published_at) ,published_hour
)

select * from daily_stats
