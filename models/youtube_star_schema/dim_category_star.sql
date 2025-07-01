{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}

create or replace table `upheld-momentum-463013-v7`.`dbt_tdereli_youtube_star_schema`.`dim_category_star`

SELECT
  DISTINCT category_id,
  category_name
FROM {{ ref('stg_youtube_trending') }}
