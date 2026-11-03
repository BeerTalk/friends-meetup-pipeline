-- Fails if any friendship appears twice in the same snapshot.
select user_a, user_b, snapshot, count(*) as n
from {{ ref('stg_friendships') }}
group by 1, 2, 3
having count(*) > 1
