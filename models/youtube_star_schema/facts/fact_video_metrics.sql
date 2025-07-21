{{ config(
    materialized='view',
    schema='youtube_star_schema'
) }}

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_youtube_trending') }}
),

channel_info AS (
    SELECT *
    FROM {{ source('dbt_tdereli', 'channel_info_enriched') }}
)

SELECT
  stg.video_id,
  stg.category_id,
  ci.channel_id,
  ci.channel_title,
  ci.subscriber_count,
  ci.total_views,
  ci.video_count,
  ci.country,
  CASE ci.country
    WHEN 'US' THEN 'United States'
    WHEN 'CA' THEN 'Canada'
    WHEN 'GB' THEN 'United Kingdom'
    WHEN 'AU' THEN 'Australia'
    WHEN 'IN' THEN 'India'
    WHEN 'JP' THEN 'Japan'
    WHEN 'KR' THEN 'South Korea'
    WHEN 'BR' THEN 'Brazil'
    WHEN 'MX' THEN 'Mexico'
    WHEN 'FR' THEN 'France'
    WHEN 'DE' THEN 'Germany'
    WHEN 'RU' THEN 'Russia'
    WHEN 'IT' THEN 'Italy'
    WHEN 'ES' THEN 'Spain'
    WHEN 'AR' THEN 'Argentina'
    WHEN 'CO' THEN 'Colombia'
    WHEN 'CL' THEN 'Chile'
    WHEN 'NL' THEN 'Netherlands'
    WHEN 'TR' THEN 'Turkey'
    WHEN 'SA' THEN 'Saudi Arabia'
    WHEN 'AE' THEN 'United Arab Emirates'
    WHEN 'EG' THEN 'Egypt'
    WHEN 'ID' THEN 'Indonesia'
    WHEN 'MY' THEN 'Malaysia'
    WHEN 'TH' THEN 'Thailand'
    WHEN 'VN' THEN 'Vietnam'
    WHEN 'SG' THEN 'Singapore'
    WHEN 'NG' THEN 'Nigeria'
    WHEN 'KE' THEN 'Kenya'
    WHEN 'ZA' THEN 'South Africa'
    WHEN 'PK' THEN 'Pakistan'
    WHEN 'BD' THEN 'Bangladesh'
    WHEN 'UA' THEN 'Ukraine'
    WHEN 'PL' THEN 'Poland'
    WHEN 'SE' THEN 'Sweden'
    WHEN 'CH' THEN 'Switzerland'
    WHEN 'BE' THEN 'Belgium'
    WHEN 'NO' THEN 'Norway'
    WHEN 'DK' THEN 'Denmark'
    WHEN 'FI' THEN 'Finland'
    WHEN 'IE' THEN 'Ireland'
    WHEN 'NZ' THEN 'New Zealand'
    WHEN 'PH' THEN 'Philippines'
    WHEN 'HK' THEN 'Hong Kong'
    WHEN 'TW' THEN 'Taiwan'
    WHEN 'IL' THEN 'Israel'
    WHEN 'RO' THEN 'Romania'
    WHEN 'HU' THEN 'Hungary'
    WHEN 'CZ' THEN 'Czech Republic'
    WHEN 'GR' THEN 'Greece'
    WHEN 'PT' THEN 'Portugal'
    WHEN 'SK' THEN 'Slovakia'
    WHEN 'AT' THEN 'Austria'
    WHEN 'BY' THEN 'Belarus'
    WHEN 'PR' THEN 'Puerto Rico'
    WHEN 'KZ' THEN 'Kazakhstan'
    WHEN 'IQ' THEN 'Iraq'
    WHEN 'QA' THEN 'Qatar'
    WHEN 'LB' THEN 'Lebanon'
    WHEN 'KW' THEN 'Kuwait'
    WHEN 'GE' THEN 'Georgia'
    WHEN 'FO' THEN 'Faroe Islands'
    WHEN 'DO' THEN 'Dominican Republic'
    WHEN 'LV' THEN 'Latvia'
    WHEN 'EC' THEN 'Ecuador'
    WHEN 'PE' THEN 'Peru'
    WHEN 'MD' THEN 'Moldova'
    WHEN 'GH' THEN 'Ghana'
    WHEN 'TZ' THEN 'Tanzania'
    WHEN 'CY' THEN 'Cyprus'
    WHEN 'MC' THEN 'Monaco'
    WHEN 'JO' THEN 'Jordan'
    WHEN 'MA' THEN 'Morocco'
    WHEN 'VE' THEN 'Venezuela'
    WHEN 'LK' THEN 'Sri Lanka'
    WHEN 'SV' THEN 'El Salvador'
    WHEN 'UY' THEN 'Uruguay'
    WHEN 'PA' THEN 'Panama'
    WHEN 'RS' THEN 'Serbia'
    WHEN 'GT' THEN 'Guatemala'
    WHEN 'AG' THEN 'Antigua and Barbuda'
    WHEN 'ME' THEN 'Montenegro'
    WHEN 'PY' THEN 'Paraguay'
    WHEN 'OM' THEN 'Oman'
    WHEN 'SY' THEN 'Syria'
    ELSE 'Unknown'
  END AS country_name,
  stg.published_at,
  stg.load_date,
  DATE_DIFF(DATE(load_date), DATE(published_at), DAY) AS days_until_it_became_trending,
  stg.view_count AS views,
  stg.like_count AS likes,
  stg.comment_count AS comments,
  stg.duration_hours,
  stg.duration_minutes,
  stg.duration_seconds
FROM stg
LEFT JOIN channel_info ci
  ON stg.video_id = ci.video_id
