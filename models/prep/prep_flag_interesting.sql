{{ config(materialized='view') }}

with base as (
    select *
    from {{ ref('stg_youtube_trending') }}
),


flags as (
    select
        *,
        view_count > 1000000 as is_viral,
        like_count > 100000 as is_highly_liked,
        comment_count > 50000 as is_highly_commented,
        duration_seconds > 600 as is_long_form
    from base
)

select 
    video_id,
    title,
    channel_title,
    category_id,
    published_at,
    load_date,
    view_count,
    like_count,
    comment_count,
    duration_seconds,
    duration_minutes,
    duration_hours,
    is_viral,
    is_highly_liked,
    is_highly_commented,
    is_long_form
from flags
