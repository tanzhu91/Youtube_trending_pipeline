{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('prep_video_metrics') }}
),

category_stats as (
    select
        date(load_date) as date,
        FORMAT_TIMESTAMP('%H:%M', date) AS load_hour,
        category_name,
        count(*) as video_count,
        sum(view_count) as total_views
    from base
    group by 1, 2
)

select *
from category_stats
order by date, total_views desc
