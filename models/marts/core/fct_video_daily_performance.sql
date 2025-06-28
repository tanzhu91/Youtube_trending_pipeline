{{ config(materialized='table') }}

with base as (
    select * from {{ ref('prep_video_metrics') }}
),

daily_stats as (
    select
        DATE(published_at) as published_at,
        SUM(view_count) as sum_views,
        SUM(like_count) as sum_likes,
        SUM(comment_count) as sum_comments
    from base
    group by 1
)
select * from daily_stats
