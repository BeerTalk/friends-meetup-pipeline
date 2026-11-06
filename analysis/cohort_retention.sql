-- Cohort retention: for each signup month, percent of users are still active
-- Run with: bq query --use_legacy_sql=false --max_rows=200 < analysis/cohort_retention.sql

with user_months as (
    -- 1. one row per user per month since active
    select distinct
        user_id,
        date_trunc(date(ts_utc),month) as activity_month
    from `analytics.stg_checkins`
),

cohorts as (
    -- 2. cohort_month is the first month a user was active
    select
        user_id,
        date_trunc(min(activity_month),month) as cohort_month
    from user_months
    group by user_id
),

activity as (
    -- 3. for every active month, how many months after signup is it
    select
        c.cohort_month,
        date_diff(u.activity_month, c.cohort_month,month) as months_since,
        u.user_id
    from user_months as u
    join cohorts as c
        on u.user_id = c.user_id
),

cohort_size as (
    -- 4: how many users started in each cohort (the 100% baseline)
    select
        cohort_month,
        count(*) as users
    from cohorts
    group by cohort_month
)

-- step 5: active users per cohort per month, and % of the cohort

select
    a.cohort_month,
    a.months_since,
    s.users as cohort_users,
    count(distinct a.user_id) as active_users,
    round(100 * count(distinct a.user_id) / s.users, 1) as pct_retained
from activity as a
join cohort_size as s
    on a.cohort_month = s.cohort_month
group by a.cohort_month, a.months_since, s.users
order by a.cohort_month, a.months_since