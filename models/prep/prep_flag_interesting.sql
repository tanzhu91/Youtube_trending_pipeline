{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('stg_youtube_trending') }}
),

flags as (
    select
        *,
        view_count > 1000000 as is_viral
    from base
),

region_counts as (
    select
        video_id,
        count(distinct default_language) as region_count
    from base
    group by video_id
),

combined as (
    select
        f.*,
        rc.region_count,
        rc.region_count > 1 as is_multi_region
    from flags f
    left join region_counts rc using (video_id)
)

select * from combined
