{{ config(materialized='table') }}

with base as (
    select * from {{ ref('prep_video_metrics') }}
),

daily_stats as (
    select
        video_id,
        title,
        channel_title,
        category_name,
        DATE(load_date) as date,
        max(view_count) as max_views,
        max(like_count) as max_likes,
        max(comment_count) as max_comments,
        count(*) as times_seen,
        count(distinct published_at) as publish_dates_count
    from base
    group by video_id, title, channel_title, category_name, DATE(load_date)
)

select * from daily_stats
