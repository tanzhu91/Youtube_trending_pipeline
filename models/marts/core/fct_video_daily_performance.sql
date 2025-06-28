{{ config(materialized='table') }}

with base as (
    select * from {{ ref('prep_video_metrics') }}
),

daily_stats as (
    select
        DATE(load_date) as load_date,
        load_hour,
        DATE(published_at) as published_at,
        published_hour,
        title,
        channel_title,
        category_name,
        SUM(view_count) as sum_views,
        SUM(like_count) as as sum_likes,
        SUM(comment_count) as sum_comments
    from base
    group by DATE(load_date) , load_hour, DATE(published_at) , published_hour ,video_id, title, channel_title, category_name
)

select * from daily_stats
