{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}


SELECT
  video_id,
  category_id,
  default_language,
  DATE(published_at) AS publish_date,
  DATE(load_date) AS load_date,
  SUM(view_count) AS views,
  SUM(like_count) AS likes,
  SUM(comment_count) AS comments,
  DATE_DIFF(DATE(load_date), DATE(publish_date), DAY) AS trending_time
FROM {{ ref('stg_youtube_trending') }}
GROUP BY 1, 2, 3, 4
