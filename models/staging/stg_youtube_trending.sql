{{ config(materialized='view') }}

with base as (
    select
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
transformed as (
    select
        video_id,
        title,
        description,
        channel_title,
        published_at,
        EXTRACT(YEAR  FROM published_at) as published_year,
        EXTRACT(MONTH FROM published_at) as published_month,
        EXTRACT(DAY   FROM published_at) as published_day,
        EXTRACT(HOUR  FROM published_at) as published_hour,
        category_id,
        CASE category_id
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
        duration_seconds / 60.0 AS duration_minutes,
        duration_seconds / 3600.0 AS duration_hours,
        view_count,
        like_count,
        comment_count
    from base
)

select * from transformed
