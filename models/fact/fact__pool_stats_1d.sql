drop table if exists fact.pool_stats_1d cascade
;

create table fact.pool_stats_1d as (

with src_block as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		-- For standardization across models, select block hash as the natural key
		  trim(block_hash::varchar) as block_key
		, blockheight::int as blockheight
		, timestamp::date as event_date
		, timestamp::timestamp as event_timestamp
		, trim(pool_key::varchar) as pool_key
		-- Create flag for difficulty adjustments (derived here to avoid verbose transformations)
		, (case
			when round(blockheight::decimal / 2016::decimal, 9)
				= floor(blockheight::decimal / 2016::decimal)
			then true
			else false
		end)::boolean as is_difficulty_adjustment
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
, deduplicate_block as (
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

, src_hashrate as (
	select distinct
		  date::date as event_date
		, trim(pool_key::varchar) as pool_key
		-- Label this hashrate as reported (vs estimated)
		, hashrate::decimal(30, 0) as reported_hashrate
		, current_timestamp as audit_created_timestamp
	from src.hashrate
	where trim(pool_key::varchar) is not null
)

, deduplicate_hashrate as (
	select *
	from (
		select
			  *
			, row_number() over (
				partition by event_date, pool_key
				order by audit_created_timestamp desc
			) as dedupe_rn
		from src_hashrate
	)
	where dedupe_rn = 1
)

-- Derive daily pool-level aggregates
, aggregate_pool as (
	select
		  event_date
		, pool_key
		-- If any block has a difficulty adjustment return true, else false
		, bool_or(is_difficulty_adjustment) as has_difficulty_adjustment
		, count(distinct block_key) as block_count
		, min(blockheight) as blockheight_first
		, max(blockheight) as blockheight_last
		, sum(reward_subsidy) as reward_subsidy_sum
		, sum(reward_tx_fee_sum) as reward_tx_fee_sum
		, sum(tx_count) as tx_count
		, max(audit_created_timestamp) as audit_created_timestamp
	from deduplicate_block
	group by event_date, pool_key
)

-- Derive daily network-level aggregates (needed for difficulty adjustment block)
, aggregate_network as (
	select
		  event_date
		, bool_or(has_difficulty_adjustment) as has_difficulty_adjustment
		, min(blockheight_first) as blockheight_first
		, max(blockheight_last) as blockheight_last
	from aggregate_pool
	group by event_date
)

, handle_difficulty_adjustments as (
	select
		  a.event_date
		, d1.difficulty as difficulty_first
		, d2.difficulty as difficulty_last
		-- Derive a blended difficulty level, weighted by time spent at each level during the day
	  	, ((da.difficulty_first::decimal * da.duration_first_sec::decimal / 86400::decimal)
			+ (da.difficulty_last::decimal * da.duration_last_sec::decimal / 86400::decimal)
			)::decimal(30, 9) as difficulty_weighted_avg
	from aggregate_network as a
	-- Join to first & last block on each date for difficulty of the last block
	inner join deduplicate_block as d1
		on d1.blockheight = a.blockheight_first
	inner join deduplicate_block as d2
		on d2.blockheight = a.blockheight_last
	-- Join to difficulty adjustment days for special handling
	left join (
		select
			  a.event_date
			, d1.difficulty as difficulty_first
			, d2.difficulty as difficulty_last
			-- Calculate seconds spent at each difficulty level on adjustment date
			, abs(extract(epoch from d2.event_timestamp - a.event_date)) as duration_first_sec
			, abs(extract(epoch from a.event_date + interval '1 day' - d2.event_timestamp)) as duration_last_sec
		from aggregate_network as a
		-- Join to the first block of each date and the difficulty adjustment block
		inner join deduplicate_block as d1
			on d1.blockheight = a.blockheight_first
		inner join deduplicate_block as d2
			on d2.event_date = a.event_date
		where a.has_difficulty_adjustment = true
		and d2.is_difficulty_adjustment = true
	) as da
		on da.event_date = a.event_date
)

, relations as (
	select
		  dd.date_id
		-- If no match was found in dim tables, use the default/unknown row (this enables inner joins downstream)
		, coalesce(dp.pool_id, '0') as pool_id
		-- If no adjustment occured, fill the weighted avg with the consistent difficulty for those days
		, coalesce(hd.difficulty_weighted_avg, hd.difficulty_first) as difficulty_weighted_avg
		, h.reported_hashrate
		, a.block_count
		, a.blockheight_first
		, a.blockheight_last
		, a.reward_subsidy_sum
		, a.reward_tx_fee_sum
		, a.tx_count
		, a.audit_created_timestamp
	from aggregate_pool as a
	inner join handle_difficulty_adjustments as hd
		on hd.event_date = a.event_date
	left join deduplicate_hashrate as h
		on h.event_date = a.event_date
		and h.pool_key = a.pool_key
	inner join dim.date as dd
		on dd.date = a.event_date
	-- Left join dims (other than date) in case no match is found
	left join dim.pool as dp
		on dp.pool_key = a.pool_key
)

-- Specify final data types, add derived cols
, standardize as (
	select
		  date_id::varchar(512) as date_id
		, pool_id::varchar(512) as pool_id
		-- Reported hashrate only available for some pools
		, reported_hashrate::decimal(30, 9) as reported_hashrate
		, (reported_hashrate::decimal / pow(10, 12)::decimal)::decimal(30, 12) as reported_hashrate_th
		, (reported_hashrate::decimal / pow(10, 15)::decimal)::decimal(30, 15) as reported_hashrate_ph
		, (reported_hashrate::decimal / pow(10, 18)::decimal)::decimal(30, 18) as reported_hashrate_eh
		-- Estimate pool hashrate as block count * difficulty * 2^32 / seconds in a day
		, (case
			when block_count = 0 then 0
			else block_count::decimal * difficulty_weighted_avg::decimal
				* pow(2, 32)::decimal / 86400::decimal
		end)::decimal(30, 0) as est_hashrate
		-- Add common hashrate scales
		, (case
			when block_count = 0 then 0
			else block_count::decimal * difficulty_weighted_avg::decimal
				* pow(2, 32)::decimal / 86400::decimal / pow(10, 12)::decimal
		end)::decimal(30, 12) as est_hashrate_th
		, (case
			when block_count = 0 then 0
			else block_count::decimal * difficulty_weighted_avg::decimal
				* pow(2, 32)::decimal / 86400::decimal / pow(10, 15)::decimal
		end)::decimal(30, 15) as est_hashrate_ph
		, (case
			when block_count = 0 then 0
			else block_count::decimal * difficulty_weighted_avg::decimal
				* pow(2, 32)::decimal / 86400::decimal / pow(10, 18)::decimal
		end)::decimal(30, 18) as est_hashrate_eh
		, block_count::int as block_count
		, blockheight_first::int as blockheight_first
		, blockheight_last::int as blockheight_last
		, (reward_subsidy_sum + reward_tx_fee_sum)::bigint as reward_mining_sum
		, reward_subsidy_sum::bigint as reward_subsidy_sum
		, reward_tx_fee_sum::bigint as reward_tx_fee_sum
		, tx_count::int as tx_count
		-- Add BTC scale numbers (vs Sats scale)
		, (100 * reward_tx_fee_sum::decimal / (reward_subsidy_sum + reward_tx_fee_sum)::decimal
			)::decimal(12, 9) as reward_tx_fee_pct
		, ((reward_subsidy_sum + reward_tx_fee_sum)::decimal / pow(10, 8)::decimal
			)::decimal(16, 8) as reward_mining_sum_btc
		, (reward_subsidy_sum::decimal / pow(10, 8)::decimal
			)::decimal(16, 8) as reward_subsidy_sum_btc
		, (reward_tx_fee_sum::decimal / pow(10, 8)::decimal
			)::decimal(16, 8) as reward_tx_fee_sum_btc
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
from fact.pool_stats_1d
order by date_id, block_count desc
limit 1000
;
