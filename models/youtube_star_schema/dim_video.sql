{{ config(
    materialized='table',
    schema='youtube_star_schema'
) }}

SELECT
  DISTINCT video_id,
  title,
  channel_id,
  publish_time,
  duration_hours
FROM {{ ref('stg_youtube_trending') }}
