{{ config(materialized='view') }}

with base as (
    select
        load_date,
        video_id,
        title,
        description,
        channel_title,
        published_at,
        category_id,
        default_language,
        tags,
        duration_seconds,
        view_count,
        like_count,
        comment_count
    from {{ source('youtube_source', 'youtube_trending_videos') }}
),

ranked as (
    select
        *,
        ROW_NUMBER() OVER (
            PARTITION BY video_id
            ORDER BY load_date ASC
        ) as row_num
    from base
),

deduplicated as (
    select *
    from ranked
    where row_num = 1
),

transformed as (
    select
        published_at,
        load_date,
        video_id,
        title,
        description,
        channel_title,
        category_id,
        CASE CAST(category_id as INT64)
          WHEN 1 THEN 'Film & Animation'
          WHEN 2 THEN 'Autos & Vehicles'
          WHEN 10 THEN 'Music'
          WHEN 15 THEN 'Pets & Animals'
          WHEN 17 THEN 'Sports'
          WHEN 18 THEN 'Short Movies'
          WHEN 19 THEN 'Travel & Events'
          WHEN 20 THEN 'Gaming'
          WHEN 22 THEN 'People & Blogs'
          WHEN 23 THEN 'Comedy'
          WHEN 24 THEN 'Entertainment'
          WHEN 25 THEN 'News & Politics'
          WHEN 26 THEN 'Howto & Style'
          WHEN 27 THEN 'Education'
          WHEN 28 THEN 'Science & Technology'
          WHEN 29 THEN 'Nonprofits & Activism'
          ELSE 'Unknown'
        END AS category_name,
        default_language,
        tags,
        duration_seconds,
        ROUND(duration_seconds / 60.0 ,3) AS duration_minutes,
        ROUND(duration_seconds / 3600.0 ,3) AS duration_hours,
        view_count,
        like_count,
        comment_count
    from deduplicated
)

select * from transformed
