{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}



SELECT
  DISTINCT video_id,
  published_at,
  load_date,
  DATE_DIFF(DATE(load_date), DATE(published_at), DAY) AS days_until_it_became_trending
FROM {{ ref('stg_youtube_trending') }}
group by 1,2,3