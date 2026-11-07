-- One row per user. how they moved and socialised in first 30 days
-- label churn: no check-in in days 30 to 60
 
{{ config(
    materialized='table',
    cluster_by=['user_id']
) }}
 
with retention as (
    select *
    from {{ ref('user_retention') }}
    where eligible   -- only users we can observe for 60 days 
),
 
first_30d_checkins as (
    -- every check-in in each user's first 30 days 
    select
        c.user_id,
        c.venue_id,
        c.h3_r9,
        c.geo,
        c.ts_local
    from {{ ref('stg_checkins') }} as c
    join retention as r
        on r.user_id = c.user_id
    -- compare UTC with UTC, same window as user_retention
    where c.ts_utc < timestamp_add(r.first_checkin, interval 30 day)
),
 
centres as (
    -- each user's centre point in their first 30 days
    select
        user_id,
        st_centroid_agg(geo) as centre
    from first_30d_checkins
    group by user_id
),
 
geo_features as (
    select
        f.user_id,
        count(distinct f.venue_id) as unique_venues,
        count(distinct f.h3_r9) as unique_cells,
        -- radius of gyration in km: typical distance from their centre
        sqrt(avg(pow(st_distance(f.geo, c.centre), 2))) / 1000 as radius_km,
        -- share of checkins on a weekend; BigQuery dayofweek: 1 = Sunday, 7 = Saturday
        countif(extract(dayofweek from f.ts_local) in (1, 7)) / count(*) as weekend_ratio
    from first_30d_checkins as f
    join centres as c
        on c.user_id = f.user_id
    group by f.user_id
),
 
friend_counts as (
    -- each friendship is stored as user_a < user_b, so list it for both
    select user_id, count(*) as n_friends
    from (
        select user_a as user_id from {{ ref('stg_friendships') }}
        union all
        select user_b as user_id from {{ ref('stg_friendships') }}
    )
    group by user_id
)
 
select
    r.user_id,
    r.activity_band,
    r.checkins_30_days,
    r.days_active_30_days,
    g.unique_venues,
    g.unique_cells,
    round(g.radius_km, 2) as radius_km,
    round(g.weekend_ratio, 3) as weekend_ratio,
    coalesce(fc.n_friends, 0) as n_friends,
    r.met_friend_first_30d,
    -- label churn = did not come back in days 30 to 60
    not r.retained_month_2 as churned
from retention as r
join geo_features as g
    on g.user_id = r.user_id
left join friend_counts as fc
    on fc.user_id = r.user_id