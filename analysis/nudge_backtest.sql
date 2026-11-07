-- Nudge backtest, how many "your friend is nearby" alerts per week,
--  how often did the friends actually meet within the next 7 days

with checkins as (
    select
        user_id,
        venue_id,
        ts_utc,
        geo,
        div(unix_seconds(ts_utc), 3600) as hour_bucket
    from `analytics.stg_checkins`
),

bounds as (
    -- triggers in the final 7 days can't show a follow-on
    select max(ts_utc) as data_end from checkins
),

-- 1. near misses = two friends within 1 km and 60 minutes, but at different venues
near_misses as (
    select
        f.user_a,
        f.user_b,
        a.ts_utc as ts_a,
        b.ts_utc as ts_b,
        st_distance(a.geo, b.geo) as metres_apart
    from `analytics.stg_friendships` as f
    join checkins as a
        on a.user_id = f.user_a
    join checkins as b
        on b.user_id = f.user_b
        and b.hour_bucket between a.hour_bucket - 1 and a.hour_bucket + 1
    where abs(timestamp_diff(a.ts_utc, b.ts_utc, minute)) <= 60
        --  diff venues
        and a.venue_id <> b.venue_id
        -- within 1 km
        and st_dwithin(a.geo, b.geo, 1000)
),

--  2: at most one alert per friend pair per day, spam = BAD
triggers as (
    select
        user_a,
        user_b,
        date(least(ts_a, ts_b)) as trigger_date,
        min(least(ts_a, ts_b)) as trigger_at,
        min(metres_apart) as metres_apart
    from near_misses
    -- one row per pair per day
    group by user_a, user_b, date(least(ts_a, ts_b))
),

-- 3: check if the pair meet up within 7 days after the alert
follow_on as (
    select
        t.*,
        exists (
            select 1
            from `analytics.meetups` as m
            where m.user_a = t.user_a
                and m.user_b = t.user_b
                -- TODO: met after the trigger, and no more than 7 days after it
                and m.met_at > t.trigger_at
                and m.met_at <= timestamp_add(t.trigger_at, interval 7 day)
        ) as met_within_7d
    from triggers as t
    cross join bounds as b
    -- only triggers with a 7 days of data after them
    where t.trigger_at <= timestamp_sub(b.data_end, interval 7 day)
)

-- 4. weekly volume n follow-on rate
select
    date_trunc(trigger_date, week(monday)) as week,
    count(*) as triggers,
    -- % of triggers followed by a meetup within 7 days
    round(100 * avg(cast(met_within_7d as int64)),1) as pct_met_within_7d
from follow_on
group by week
order by week