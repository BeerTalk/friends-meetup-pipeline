-- One row per friendship per snapshot, stored once with user_a < user_b.
select distinct
    least(user_a, user_b) as user_a,
    greatest(user_a, user_b) as user_b,
    'before' as snapshot
from {{ source('raw', 'friendships_before') }}
where user_a != user_b

union all

select distinct
    least(user_a, user_b) as user_a,
    greatest(user_a, user_b) as user_b,
    'after' as snapshot
from {{ source('raw', 'friendships_after') }}
where user_a != user_b
