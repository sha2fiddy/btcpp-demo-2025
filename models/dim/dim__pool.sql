drop table if exists dim.pool
;

create table dim.pool as (

with pool_data as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		  trim(pool_key) as pool_key
		, trim(pool_name) as pool_name
		, trim(pool_url) as pool_url
		, is_antpool_friend_custodian
		, is_antpool_friend_template
	from src.pool
	where trim(pool_key) is not null
)

-- Specify final data types, create surrogate id for dim table
, standardize as (
	select
		-- Another good place for a dbt macro
		  md5('surrogate' || pool_key)::varchar(512) as pool_id
		, pool_key::varchar(256) as pool_key
		, coalesce(pool_name, '[Unknown]')::varchar(256) as pool_name
		, coalesce(pool_url, '[Unknown]')::varchar(512) as pool_url
		, is_antpool_friend_custodian::boolean as is_antpool_friend_custodian
		, is_antpool_friend_template::boolean as is_antpool_friend_template
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from pool_data
)

-- Best practice is to deduplicate records based on some metadata/audit timestamp
, deduplicated as (
	select
		  pool_id
		, pool_key
		, pool_name
		, pool_url
		, is_antpool_friend_custodian
		, is_antpool_friend_template
		, audit_created_timestamp
	from (
		select
			  *
			, row_number() over (
				-- This defines the data granularity (unique natural key or keys)
				partition by pool_key
                -- Best practice is to deduplicate records based on an actual metadata/audit timestamp
				order by audit_created_timestamp desc
			) as dedupe_rn
		from standardize
	)
	-- Postgres does not allow you to `qualify` a row number without actually selecting it (so we can't select *)
	where dedupe_rn = 1
)

select *
from deduplicated

);

select *
from dim.pool
order by pool_key
limit 1000
;
