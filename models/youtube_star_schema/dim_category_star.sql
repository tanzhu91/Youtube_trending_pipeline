{{ config(
    materialized='table',
    schema='youtube_star_schema'
) }}

SELECT
  DISTINCT category_id,
  category_name
FROM {{ ref('stg_youtube_trending') }}
