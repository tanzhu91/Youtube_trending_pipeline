{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}



SELECT
  DISTINCT video_id,
  title,
  channel_title,
  published_at,
  load_date,
  duration_hours
FROM {{ ref('stg_youtube_trending') }}
