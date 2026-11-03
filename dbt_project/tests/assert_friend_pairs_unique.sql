-- Fails if any friendship appears more than once.
select user_a, user_b, count(*) as n
from {{ ref('stg_friendships') }}
group by 1, 2
having count(*) > 1
