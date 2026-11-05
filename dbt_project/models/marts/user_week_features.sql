-- one row per user, weeks starts from
-- input for retention analysis and churn

{{ config(
    materialized='table',
    partition_by={
        "field": "week",
        "data_type": "date"},
    cluster_by=["user_id"]

)}}

with checkins as (
    select
        user_id,venue_id, h3_r9, geo, ts_local, date(ts_local) as local_date,
        date_trunc(date(ts_local), week(monday)) as week 
    from {{ ref('stg_checkins') }}
),

week_centres as (
    select
        user_id, week, st_centroid_agg(geo) as centre
    from checkins
    group by 1,2 
),

weekly as (
    select
        c.user_id, c.week, count(*) as checkins,
        count(distinct(c.local_date)) as active_days,
        count(distinct(c.venue_id)) as unique_venues,
        count(distinct(c.h3_r9)) as unique_h3_r9,
        -- share of check-ins on a weekend, BigQuery's dayofweek is 1 = Sunday, 7 = Saturday
        countif(extract(dayofweek from c.ts_local) in(1,7))/count(*) as weekend_ratio,
        -- how far users travel from their weekly centre
        sqrt(avg(pow(st_distance(c.geo, wc.centre),2)) )/1000 as radius_km
    from checkins c
    left join week_centres wc -- keeps every checkin even w no matching center
    on c.user_id = wc.user_id and c.week = wc.week
    group by c.user_id, c.week
)

select
    concat(cast(user_id as string), '-', cast(week as string)) as user_week_key,
    *
from weekly