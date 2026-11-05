-- each meet up has a friend pair at the same venue on the same day
-- checkins within 1 hour

{{ config(
    materialized='table',
    partition_by={
        "field": "met_date",
        "data_type": "date"},
    cluster_by=["user_a","user_b"]

)}}


with checkins as (
    select
        user_id,venue_id, h3_r9, ts_utc, div(unix_seconds(ts_utc), 3600) as hour_bucket
    from {{ ref('stg_checkins') }}
),

checkins_spread as(
    select
        user_id,venue_id,ts_utc, 
        hour_bucket + offset_hours as hour_bucket
    from checkins
    cross join unnest([-1,0,1]) as offset_hours
),

friends as (
    select
        user_a, user_b
    from {{ ref('stg_friendships') }} 
),

pairs as (
    select
        c1.user_id as user_a, c2.user_id as user_b, c1.venue_id, c1.h3_r9, c1.ts_utc as ts_a, c2.ts_utc as ts_b
    from checkins as c1
    join checkins_spread as c2
        on c1.venue_id = c2.venue_id and c2.hour_bucket = c1.hour_bucket and c1.user_id < c2.user_id and abs(timestamp_diff(c1.ts_utc, c2.ts_utc, minute)) <= 60
    join friends as f
        on c1.user_id = f.user_a and c2.user_id = f.user_b
),

meetups as (
    select
        user_a,
        user_b,
        venue_id,
        any_value(h3_r9) as h3_r9,
        date(least(ts_a, ts_b)) as met_date,
        min(least(ts_a, ts_b)) as met_at,
        min(abs(timestamp_diff(ts_a, ts_b, minute))) as minutes_apart,
        count(*) as checkins_pairs
    from pairs
    group by user_a, user_b, venue_id, met_date
)

select
    concat(
        cast(user_a as string), '-',
        cast(user_b as string), '-',
        cast(venue_id as string), '-',
        cast(met_date as string)
    ) as meetup_key,
    *
from meetups
 