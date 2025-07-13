{{ config(materialized='view') }}

with base as (
    select * from {{ ref('prep_video_metrics') }}
),

daily_stats as (
    select
        DATE(published_at) as published_at,
        SUM(view_count) as sum_views,
        SUM(like_count) as sum_likes,
        SUM(comment_count) as sum_comments,
        COUNT(*) as video_count,
        SUM(duration_hours) as sum_duration_hours,
        sum(like_rate),
        sum(comment_rate)
    from base
    group by 1
)
select * from daily_stats
order by published_at desc
