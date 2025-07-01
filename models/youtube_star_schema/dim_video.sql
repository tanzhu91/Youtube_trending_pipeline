{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}


create or replace table `upheld-momentum-463013-v7`.`dbt_tdereli_youtube_star_schema`.`dim_video`

SELECT
  DISTINCT video_id,
  title,
  channel_id,
  publish_time,
  duration_hours
FROM {{ ref('stg_youtube_trending') }}
