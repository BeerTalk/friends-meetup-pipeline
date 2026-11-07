-- Which queries scanned the most data in the last 7 days
select
    user_email, -- who ran it
    statement_type,  -- SELECT, CREATE_TABLE_AS_SELECT, or
    destination_table.table_id as target_table,   -- table it wrote to (dbt models show here)
    count(*) as jobs,
    round(sum(total_bytes_billed) / pow(1024, 3), 2) as gib_billed,
    round(sum(total_bytes_billed) / pow(1024, 4) * 6.25, 4) as est_usd   -- on-demand price per TiB
from `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
where creation_time >= timestamp_sub(current_timestamp(), interval 7 day)
    and job_type = 'QUERY'
group by user_email, statement_type, target_table
order by gib_billed desc
limit 15