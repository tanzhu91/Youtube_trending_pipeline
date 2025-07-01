{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}

create or replace table `upheld-momentum-463013-v7`.`dbt_tdereli_youtube_star_schema`.`fact_video_metrics`

SELECT
  video_id,
  category_id,
  region,
  DATE(event_time) AS date_id,
  SUM(views) AS views,
  SUM(likes) AS likes,
  SUM(comments) AS comments
FROM {{ ref('stg_youtube_trending') }}
GROUP BY 1, 2, 3, 4
