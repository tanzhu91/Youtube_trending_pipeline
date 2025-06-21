{{ config(materialized='table') }}

select distinct
    category_id,
    category_name
from {{ ref('stg_youtube_trending') }}
