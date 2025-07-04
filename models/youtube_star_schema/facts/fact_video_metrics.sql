{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_youtube_trending') }}
),

channel_info AS (
    SELECT video_id, channel_id
    FROM {{ source('dbt_tdereli', 'channel_info_enriched') }}
)

SELECT
  stg.video_id,
  stg.category_id,
  ci.channel_id,
  stg.published_at,
  stg.load_date,
  stg.DATE_DIFF(DATE(load_date), DATE(published_at), DAY) AS days_until_it_became_trending,
  stg.view_count AS views,
  stg.like_count AS likes,
  stg.comment_count AS comments,
  stg.duration_hours,
  stg.duration_minutes,
  stg.duration_seconds
FROM stg
LEFT JOIN channel_info ci
  ON stg.video_id = ci.video_id
