{{ config(materialized='view', schema='youtube_star_schema') }}

SELECT DISTINCT
  title,        
  channel_title
FROM {{ ref('stg_youtube_trending') }}
