{{ config(materialized='view', schema='youtube_star_schema') }}

SELECT DISTINCT
  title,        
  channel_title
FROM {{ ref('channel_info_enriched') }}
