{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}



SELECT
  DISTINCT video_id,
  published_at,
  load_date
FROM {{ ref('stg_youtube_trending') }}
