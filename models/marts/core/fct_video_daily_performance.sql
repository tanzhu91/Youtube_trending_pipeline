{{ config(materialized='table') }}

with base as (
    select * from {{ ref('prep_video_metrics') }}
),

daily_stats as (
    select
        DATE(load_date) as load_date,
        EXTRACT(DAY FROM load_date) AS load_day,
        DATE(published_at) as published_at,
        EXTRACT(DAY FROM published_at) AS publish_day,
        SUM(view_count) as sum_views,
        SUM(like_count) as sum_likes,
        SUM(comment_count) as sum_comments
    from base
    group by DATE(load_date), load_day, load_hour, DATE(published_at), publish_day
)
select * from daily_stats
