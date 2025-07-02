{{ config(materialized='view', schema='dbt_tdereli_youtube_star_schema') }}

SELECT
  video_id,
  category_id,
  default_language,
  view_count,
  like_count,
  comment_count,
  DATE(published_at) AS published_date,
  DATE(load_date) AS trending_date,
  DATE_DIFF(DATE(load_date), DATE(published_at), DAY) AS trending_time
FROM {{ ref('stg_youtube_trending') }}

