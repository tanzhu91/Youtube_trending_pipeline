{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}



SELECT
  DISTINCT video_id,
  published_at,
  load_date,
  duration_hours,
  duration_minutes,
  duration_seconds
FROM {{ ref('stg_youtube_trending') }}
