{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('prep_flag_interesting') }}
),

category_stats as (
    select
        date(load_date) as date,
        category_name,
        count(*) as video_count,
        sum(view_count) as total_views
    from base
    group by 1, 2
)

select *
from category_stats
order by date, total_views desc
