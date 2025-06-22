{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('prep_flag_interesting') }}
),

ranked as (
    select
        *,
        row_number() over (
            partition by date(load_date)
            order by view_count desc
        ) as view_rank
    from base
)

select *
from ranked
where view_rank <= 5
