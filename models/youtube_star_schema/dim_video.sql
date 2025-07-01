{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}



SELECT
  DISTINCT video_id,
  title,
  channel_title,
  publish_at,
  duration_hours
FROM {{ ref('stg_youtube_trending') }}
