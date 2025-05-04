drop table if exists dim.pool
;

create table dim.pool as (

with src_pool as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		  trim(pool_key::varchar) as pool_key
		, trim(pool_name::varchar) as pool_name
		, trim(pool_url::varchar) as pool_url
		, is_antpool_friend_custodian::boolean as is_antpool_friend_custodian
		, is_antpool_friend_template::boolean as is_antpool_friend_template
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from src.pool
	where trim(pool_key) is not null
)

-- Remove any duplicate data that could exist in src layer
, deduplicate as (
	select *
	from (
		select
			  *
			, row_number() over (
				-- The partition defines the data granularity (unique natural key or keys)
				partition by pool_key
                -- Best practice is to deduplicate records based on an actual metadata/audit timestamp
				order by audit_created_timestamp desc
			) as dedupe_rn
		from src_pool
	)
	where dedupe_rn = 1
)

-- Specify final data types, derived cols, create surrogate id for dim table
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
		, audit_created_timestamp::timestamp as audit_created_timestamp
	from deduplicate

	-- Create any default or special rows for dim table ('[Unknown]', '[N/A]', '[All]', etc)
	union all
	select
		  '0'::varchar(512) as pool_id
		, '[Unknown]'::varchar(256) as pool_key
		, '[Unknown]'::varchar(256) as pool_name
		, '[Unknown]'::varchar(512) as pool_url
		, null::boolean as is_antpool_friend_custodian
		, null::boolean as is_antpool_friend_template
		, null::timestamp as audit_created_timestamp
)

select *
from standardize

);

select *
from dim.pool
order by pool_key
limit 1000
;
