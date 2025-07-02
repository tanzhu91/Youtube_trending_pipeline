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
  DATE_DIFF(DATE(load_date), DATE(published_at), DAY) AS trending_time,
  SUM(view_count) AS views,
  SUM(like_count) AS likes,
  SUM(comment_count) AS comments
FROM {{ ref('stg_youtube_trending') }}
GROUP BY 1, 2, 3, 4, 5 , 6
