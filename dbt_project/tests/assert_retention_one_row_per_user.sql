-- Fails if the retention table does not have exactly one row per use w checkikns

select *
from (
    select
        (select count(distinct user_id) from {{ ref('stg_checkins') }}) as users_with_checkins,
        (select count(*) from {{ ref('user_retention') }}) as users_in_retention
)
where users_with_checkins != users_in_retention