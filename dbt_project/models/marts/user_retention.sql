-- activity in first 30 days
-- see if the friend meetup  come back in day 30 to 60

{{ config(
    materialized='table',
    cluster_by=["user_id"]
) }}

with checkins as (
    select
        user_id, ts_utc
    from {{ ref('stg_checkins') }}
),

bounds as (
    select max(ts_utc) as date_end from checkins
),

first as (
    select
        user_id,
        min(ts_utc) as first_checkin,
        max(ts_utc) as last_checkin
    from checkins
    group by user_id
),

first_30_days as (
    select
        c.user_id,
        count(*) as checkins_30_days,
        count(distinct date(c.ts_utc)) as days_active_30_days
    from checkins as c
    join first as f
        on c.user_id = f.user_id
    where c.ts_utc < timestamp_add(f.first_checkin, interval 30 day)
    group by c.user_id
),
day_30_to_60 as (
    select distinct c.user_id
    from checkins as c
    join first as f
        on c.user_id = f.user_id
    where c.ts_utc >= timestamp_add(f.first_checkin, interval 30 day)
        and c.ts_utc < timestamp_add(f.first_checkin, interval 60 day)
),

user_meetups as (
    select user_a as user_id, met_at from {{ ref('meetups') }}
    union all
    select user_b as user_id, met_at from {{ ref('meetups') }}
),

first_30_days_meetups as (
    select distinct u.user_id
    from user_meetups as u
    join first as f
        on u.user_id = f.user_id
    where u.met_at < timestamp_add(f.first_checkin, interval 30 day)
)

select
    f.user_id,
    f.first_checkin,
    f.last_checkin,
    a.checkins_30_days,
    a.days_active_30_days,
    case
        when a.checkins_30_days < 5 then '1-4'
        when a.checkins_30_days < 15 then '5-14'
        else '15+'
    end as activity_band,
    m.user_id is not null as met_friend_first_30d,
    f.first_checkin <= timestamp_sub(b.date_end, interval 60 day) as eligible,
    r.user_id is not null as retained_month_2
from first as f
cross join bounds as b
join first_30_days as a
    on a.user_id = f.user_id
left join first_30_days_meetups as m
    on m.user_id = f.user_id
left join day_30_to_60 as r
    on r.user_id = f.user_id