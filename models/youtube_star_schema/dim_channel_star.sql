{{ config(materialized='view', schema='youtube_star_schema') }}

SELECT DISTINCT
  channel_id,        
  channel_title
FROM {{ ref('channel_info_enriched') }}
