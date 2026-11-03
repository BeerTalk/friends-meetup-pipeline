-- One row per friendship, stored once with user_a < user_b (raw data lists both directions).
select distinct
    least(user_a, user_b) as user_a,
    greatest(user_a, user_b) as user_b
from {{ source('raw', 'friendships') }}
where user_a != user_b
