drop table if exists dim.block
;

create table dim.block as (

with block_data as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		-- For standardization across models, select block hash again as the natural key
		  trim(block_hash::varchar) as block_key
		, trim(block_hash::varchar) as block_hash
		, trim(prev_block_hash::varchar) as prev_block_hash
		, blockheight::int as blockheight
	from src.block
	where trim(block_hash) is not null
)

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
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from block_data
)

-- Best practice is to deduplicate records based on some metadata/audit timestamp
, deduplicated as (
	select
		  block_id
		, block_key
		, block_hash
		, prev_block_hash
		, is_subsidy_halving
		, is_difficulty_adjustment
		, audit_created_timestamp
	from (
		select
			  *
			, row_number() over (
				-- This defines the data granularity (unique natural key or keys)
				partition by block_key
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
from dim.block
order by block_key
limit 1000
;
