{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}



SELECT
  video_id,
  title,
  description,
  default_language,
  tags
FROM {{ ref('stg_youtube_trending') }}