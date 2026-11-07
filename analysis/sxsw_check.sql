-- Check was the March 2010 spike in nudge triggers caused by SXSW the festival in Austin (12 to 21 March 2010) or what
-- Compares where near misses happened in the spike weeks vs the two weeks before.

with checkins as (
    -- only late Feb to late March 2010, reduce size
    select
        user_id,
        venue_id,
        ts_utc,
        geo,
        div(unix_seconds(ts_utc), 3600) as hour_bucket
    from `analytics.stg_checkins`
    where ts_utc >= timestamp '2010-02-22' and ts_utc < timestamp '2010-03-22'
),

near_misses as (
    -- same rule as the nudge, friends within 1 km and 60 minutes, different venues
    select
        f.user_a,
        f.user_b,
        date(least(a.ts_utc, b.ts_utc)) as trigger_date,
        a.geo
    from `analytics.stg_friendships` as f
    join checkins as a
        on a.user_id = f.user_a
    join checkins as b
        on b.user_id = f.user_b
        and b.hour_bucket between a.hour_bucket - 1 and a.hour_bucket + 1
    where abs(timestamp_diff(a.ts_utc, b.ts_utc, minute)) <= 60
        and a.venue_id <> b.venue_id
        and st_dwithin(a.geo, b.geo, 1000)
),

triggers as (
    -- one per pair per day, keeping one location for it
    select
        user_a,
        user_b,
        trigger_date,
        any_value(geo) as geo
    from near_misses
    group by user_a, user_b, trigger_date
)

select
    case
        when trigger_date >= '2010-03-08' then 'spike weeks (8 to 21 Mar)'
        else 'before (22 Feb to 7 Mar)'
    end as period,
    -- round to 1 decimal place = an area roughly 10 km across
    round(st_y(geo), 1) as lat,
    round(st_x(geo), 1) as lon,
    count(*) as triggers
from triggers
group by period, lat, lon
qualify row_number() over (partition by period order by count(*) desc) <= 5
order by period, triggers desc