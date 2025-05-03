drop table if exists fact.block
;

create table fact.block as (

with src_block as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		-- For standardization across models, select block hash again as the natural key
		  trim(block_hash::varchar) as block_key
		, blockheight::int as blockheight
	  	, timestamp::timestamp as event_timestamp
		, trim(pool_key::varchar) as pool_key
		, difficulty::decimal(30, 9) as difficulty
		, reward_subsidy::bigint as reward_subsidy
		, reward_tx_fee_sum::bigint as reward_tx_fee_sum
		, tx_count::int as tx_count
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from src.block
	where trim(block_hash) is not null
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
				-- Best practice is to deduplicate records based on an actual metadata/audit timestamp
				order by audit_created_timestamp desc
			) as dedupe_rn
		from src_block
	)
	-- Postgres does not allow you to `qualify` a row number without actually selecting it (so we can't select *)
	where dedupe_rn = 1
)

, relations as (
	select
		  dd.date_id
		-- If no match was found in dim tables, use the default/unknown row (this ensures inner joins downstream)
		, coalesce(db.block_id, '0') as block_id
		, coalesce(dp.pool_id, '0') as pool_id
		, d.blockheight
		, d.event_timestamp
		, d.difficulty
		, d.reward_subsidy
		, d.reward_tx_fee_sum
		, d.tx_count
		, d.audit_created_timestamp
	from deduplicate as d
	inner join dim.date as dd
		on dd.date = date(d.event_timestamp)
	-- Left join dims (other than date)
	left join dim.block as db
		on db.block_key = d.block_key
	left join dim.pool as dp
		on dp.pool_key = d.pool_key
	
)

-- Specify final data types, derived cols, create surrogate id for dim table
, standardize as (
	select
		  date_id::varchar(8) as date_id
		, block_id::varchar(512) as block_id
		, pool_id::varchar(512) as pool_id
		, blockheight::int as blockheight
		, event_timestamp::timestamp as event_timestamp
		, difficulty::decimal(30, 9) as difficulty
		, reward_subsidy::bigint as reward_subsidy
		, reward_tx_fee_sum::bigint as reward_tx_fee_sum
		, tx_count::int as tx_count
		, audit_created_timestamp::timestamp as audit_created_timestamp
	from relations
)

select *
from standardize

);

select *
from fact.block
order by blockheight
limit 1000
;
