-- Fails if any meetup stores a pair the wrong way round.
-- Pairs must always be stored with the smaller user_id first, so each pair is counted once.
select *
from {{ ref('meetups') }}
where user_a >= user_b