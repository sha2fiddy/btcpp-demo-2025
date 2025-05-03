drop table if exists dim.pool
;

create table dim.pool as (

with pool_data as (
	select distinct
		-- Apply some basic data cleaning to string fields (good use case for a dbt macro)
		  trim(pool_key) as pool_key
		, trim(pool_name) as pool_name
		, trim(pool_url) as pool_url
		, is_antpool_friend_custodian
		, is_antpool_friend_template
	from src.pool
	where trim(pool_key) is not null
)

, standardize as (
	select
		-- Create a surrogate key for dim table (a stored proc or dbt macro could be used)
		  md5('surrogate' || pool_key)::varchar(512) as pool_id
		, pool_key::varchar(256) as pool_key
		, coalesce(pool_name, '[Unknown]')::varchar(256) as pool_name
		, coalesce(pool_url, '[Unknown]')::varchar(512) as pool_url
		, is_antpool_friend_custodian::boolean as is_antpool_friend_custodian
		, is_antpool_friend_template::boolean as is_antpool_friend_template
		-- Create a dummy timestamp for deduplication; ideally there is a real audit timestamp from src data
		, current_timestamp as audit_created_timestamp
	from pool_data
)

-- Best practice is to deduplicate records based on some metadata/audit timestamp
, deduplicated as (
	select *
	from (
		select
			  *
			, row_number() over (
				partition by pool_key
				order by audit_created_timestamp desc
			) as dedupe_rn
		from standardize
	)
	where dedupe_rn = 1
)

select *
from deduplicated

);

-- Postgres has no way to exclude a single column when creating the table, so just drop the dedupe_rn
alter table dim.pool drop column dedupe_rn
;

select *
from dim.pool
order by pool_key
limit 1000
;