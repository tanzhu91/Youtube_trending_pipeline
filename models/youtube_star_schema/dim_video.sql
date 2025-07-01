{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}



SELECT
  DISTINCT video_id,
  title,
  channel_title,
  published_at,
  duration_hours
FROM {{ ref('stg_youtube_trending') }}
