{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}


SELECT
  video_id,
  category_id,
  --DATE(published_at) AS publish_date,
  --DATE(load_date) AS load_date,
  DATE_DIFF(DATE(load_date), DATE(published_at), DAY) AS days_until_it_became_trending,
  view_count AS views,
  like_count AS likes,
  comment_count AS comments,
  duration_hours,
  duration_minutes,
  duration_seconds
FROM {{ ref('stg_youtube_trending') }}
