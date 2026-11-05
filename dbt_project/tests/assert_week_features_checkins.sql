-- Fails if the weekly features do not account for every staged check-in.
-- Every check-in belongs to exactly one user-week, so the totals must match.
select *
from (
    select
        (select count(*) from {{ ref('stg_checkins') }}) as staged_checkins, -- rows of claned up check in records
        (select sum(checkins) from {{ ref('user_week_features') }}) as checkins_in_weeks -- rows of checkins by users by week
)
where staged_checkins != checkins_in_weeks