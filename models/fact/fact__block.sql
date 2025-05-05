drop table if exists fact.block cascade
;

create table fact.block as (

with src_block as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		-- For standardization across models, select block hash again as the natural key
		  trim(block_hash::varchar) as block_key
		, blockheight::int as blockheight
		, timestamp::date as event_date
	  	, timestamp::timestamp as event_timestamp
		, trim(pool_key::varchar) as pool_key
		, difficulty::decimal(30, 9) as difficulty
		, reward_subsidy::bigint as reward_subsidy
		, reward_tx_fee_sum::bigint as reward_tx_fee_sum
		, tx_count::int as tx_count
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from src.block
	where trim(block_hash::varchar) is not null
	-- Typically fact tables would build incrementally on some schedule, and a date filter would be applied accordingly
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
	-- Postgres does not allow you to `qualify` a row number without actually selecting it (so we can't select *)
	where dedupe_rn = 1
)

, derive_epochs as (
	select
		  block_key
		, subsidy_epoch
		-- Determine the block subsidy level
		, floor(5000000000::decimal / pow(2, (subsidy_epoch - 1))::decimal)::bigint as subsidy_level
		, difficulty_epoch
	from (
		select
			  block_key
			-- Determine subsidy and difficulty epochs (derived here to avoid verbose transformation)
			, (1 + floor(blockheight::decimal / 210000::decimal))::int as subsidy_epoch
			, (1 + floor(blockheight::decimal / 2016::decimal))::int as difficulty_epoch
		from deduplicate
	)
)

, relations as (
	select
		  dd.date_id
		-- If no match was found in dim tables, use the default/unknown row (this enables inner joins downstream)
		, coalesce(db.block_id, '0') as block_id
		, coalesce(dp.pool_id, '0') as pool_id
		, d.blockheight
		, d.event_timestamp
		, e.subsidy_epoch
		, e.subsidy_level
		, e.difficulty_epoch
		, d.difficulty
		, d.reward_subsidy
		, d.reward_tx_fee_sum
		, d.tx_count
		, d.audit_created_timestamp
	from deduplicate as d
	inner join derive_epochs as e
		on e.block_key = d.block_key
	inner join dim.date as dd
		on dd.date = d.event_date
	-- Left join dims (other than date) in case no match is found
	left join dim.block as db
		on db.block_key = d.block_key
	left join dim.pool as dp
		on dp.pool_key = d.pool_key
)

-- Specify final data types, add derived cols
, standardize as (
	select
		  date_id::varchar(8) as date_id
		, block_id::varchar(512) as block_id
		, pool_id::varchar(512) as pool_id
		, blockheight::int as blockheight
		, event_timestamp::timestamp as event_timestamp
		, subsidy_epoch::int as subsidy_epoch
		, subsidy_level::bigint as subsidy_level
		, (subsidy_level::decimal / pow(10, 8)::decimal)::decimal(16, 8) as subsidy_level_btc
		, difficulty_epoch::int as difficulty_epoch
		, difficulty::decimal(30, 9) as difficulty
		, (reward_subsidy + reward_tx_fee_sum)::bigint as reward_mining
		, reward_subsidy::bigint as reward_subsidy
		, reward_tx_fee_sum::bigint as reward_tx_fee_sum
		, (100 * reward_tx_fee_sum::decimal / (reward_subsidy + reward_tx_fee_sum)::decimal
			)::decimal(12, 9) as reward_tx_fee_pct
		-- Add BTC scale numbers (vs Sats scale)
		, ((reward_subsidy + reward_tx_fee_sum)::decimal / pow(10, 8)::decimal
			)::decimal(16, 8) as reward_mining_btc
		, (reward_subsidy::decimal / pow(10, 8)::decimal
			)::decimal(16, 8) as reward_subsidy_btc
		, (reward_tx_fee_sum::decimal / pow(10, 8)::decimal
			)::decimal(16, 8) as reward_tx_fee_sum_btc
		, tx_count::int as tx_count
		, (reward_tx_fee_sum::decimal / tx_count::decimal)::decimal(16, 3) as tx_fee_avg
		, (reward_tx_fee_sum::decimal / tx_count::decimal / pow(10, 8)::decimal
			)::decimal(19, 11) as tx_fee_avg_btc
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
