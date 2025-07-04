{{ config(materialized='view', schema='youtube_star_schema') }}

SELECT DISTINCT
  channel_id,        
  channel_titleSELECT *
FROM {{ source('dbt_tdereli', 'channel_info_enriched') }}

