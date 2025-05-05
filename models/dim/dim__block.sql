drop table if exists dim.block cascade
;

create table dim.block as (

with src_block as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		-- For standardization across models, select block hash again as the natural key
		  trim(block_hash::varchar) as block_key
		, trim(block_hash::varchar) as block_hash
		, trim(prev_block_hash::varchar) as prev_block_hash
		, blockheight::int as blockheight
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from src.block
	where trim(block_hash::varchar) is not null
)

-- Best practice is to deduplicate records based on some metadata/audit timestamp
, deduplicate as (
	select *
	from (
		select
			  *
			, row_number() over (
				-- The partition defines the data granularity (unique natural key or keys)
				partition by block_key
				-- This should be a timestamp of when source data was last updated (lacking that, when row was loaded to src table)
				order by audit_created_timestamp desc
			) as dedupe_rn
		from src_block
	)
	where dedupe_rn = 1
)

-- Specify final data types, add derived cols, create surrogate id for dim table
, standardize as (
	select
		-- Create a surrogate key for dim table (a stored proc or dbt macro could be used)
		  md5('surrogate' || block_key)::varchar(512) as block_id
		, block_key::varchar(256) as block_key
		, block_hash::varchar(256) as block_hash
		, prev_block_hash::varchar(256) as prev_block_hash
		-- Create flags for subsidy halvings and difficulty adjustments
		, (case
			when round(blockheight::decimal / 210000::decimal, 9)
				= floor(blockheight::decimal / 210000::decimal)
			then true
			else false
		end)::boolean as is_subsidy_halving
		, (case
			when round(blockheight::decimal / 2016::decimal, 9)
				= floor(blockheight::decimal / 2016::decimal)
			then true
			else false
		end)::boolean as is_difficulty_adjustment
		, audit_created_timestamp::timestamp as audit_created_timestamp
	from deduplicate

	-- Create any default or special rows for dim table ('[Unknown]', '[N/A]', '[All]', etc)
	union all
	select
		  '0'::varchar(512) as block_id
		, '[Unknown]'::varchar(256) as block_key
		, '[Unknown]'::varchar(256) as block_hash
		, '[Unknown]'::varchar(256) as prev_block_hash
		, null::boolean as is_subsidy_halving
		, null::boolean is_difficulty_adjustment
		, null::timestamp as audit_created_timestamp
)

select *
from standardize

);

select *
from dim.block
order by block_key
limit 1000
;
