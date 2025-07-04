{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}


SELECT
  video_id,
  category_id,
  --DATE(published_at) AS publish_date,
  --DATE(load_date) AS load_date,
  DATE_DIFF(MAX(DATE(load_date)), DATE(published_at), DAY) AS days_until_it_became_trending,
  view_count AS views,
  like_count AS likes,
  comment_count AS comments,
  duration_hours,
  duration_minutes,
  duration_seconds
FROM {{ ref('stg_youtube_trending') }}
GROUP BY
  video_id,
  category_id,
  view_count,
  like_count,
  comment_count,
  duration_hours,
  duration_minutes,
  duration_seconds,
  published_at
