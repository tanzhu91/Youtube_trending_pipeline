{{ config(materialized='view', schema='youtube_star_schema') }}

SELECT DISTINCT
  channel_id,        
  channel_title
FROM {{ source('dbt_tdereli', 'channel_info_enriched') }}

