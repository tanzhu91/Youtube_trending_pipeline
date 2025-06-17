with source as (
    select * from {{ ref('stg_youtube_trending') }}
),

channel_agg as (
    select
        channel_title,
        count(*) as video_count,
        sum(view_count) as total_views,
        sum(like_count) as total_likes,
        sum(comment_count) as total_comments
    from source
    group by channel_title
)

select * from channel_agg
