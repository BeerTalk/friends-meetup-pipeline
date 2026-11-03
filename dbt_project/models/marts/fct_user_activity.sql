-- First mart: one row per user. Day 3 adds user_week_features, meetups and user_retention here.
select
    user_id,
    min(ts_utc) as first_checkin_at,
    max(ts_utc) as last_checkin_at,
    count(*) as checkins,
    count(distinct venue_id) as distinct_venues,
    count(distinct h3_r9) as distinct_cells
from {{ ref('stg_checkins') }}
group by user_id
