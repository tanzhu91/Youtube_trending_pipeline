{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('prep_video_metrics') }}
),

category_stats as (
    select
        date(published_at) as date,
        category_name,
        count(*) as video_count,
        sum(view_count) as total_views,
        SUM(like_count) as sum_likes,
        SUM(comment_count) as sum_comments,
        SUM(duration_hours) as sum_duration_hours
    from base
    group by 1, 2
)

select *
from category_stats
order by date, total_views desc
