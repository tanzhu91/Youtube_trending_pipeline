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
        DATE(load_date) as load_date,
        FORMAT_TIMESTAMP('%H:%M', load_date) AS load_hour,
        max(view_count) as max_views,
        max(like_count) as max_likes,
        max(comment_count) as max_comments,
        count(*) as times_seen,
        count(distinct published_at) as publish_dates_count
    from base
    group by video_id, title, channel_title, category_name, DATE(load_date), load_hour
)

select * from daily_stats
