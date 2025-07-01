{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}


SELECT
  video_id,
  category_id,
  default_language,
  DATE(published_at) AS date_id,
  SUM(view_count) AS views,
  SUM(like_count) AS likes,
  SUM(comment_count) AS comments
FROM {{ ref('stg_youtube_trending') }}
GROUP BY 1, 2, 3, 4
