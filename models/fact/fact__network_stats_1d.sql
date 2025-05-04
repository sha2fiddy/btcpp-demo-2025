drop table if exists fact.network_stats_1d
;

create table fact.network_stats_1d as (

with src_block as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		-- For standardization across models, select block hash as the natural key
		  trim(block_hash::varchar) as block_key
		, blockheight::int as blockheight
		, timestamp::date as event_date
		, timestamp::timestamp as event_timestamp
		, trim(pool_key::varchar) as pool_key
		-- Create flags for subsidy halvings and difficulty adjustments (derived here to avoid verbose transformations)
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
		, difficulty::decimal(30, 9) as difficulty
		, reward_subsidy::bigint as reward_subsidy
		, reward_tx_fee_sum::bigint as reward_tx_fee_sum
		, tx_count::int as tx_count
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from src.block
	where trim(block_hash) is not null
)

-- Remove any duplicate data that could exist in src layer
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
	where dedupe_rn = 1
)

-- Derive daily network-level aggregates
, aggregate as (
	select
		  event_date
		-- If any block has a halving or difficulty adjustment return true, else false
		, bool_or(is_subsidy_halving) as has_subsidy_halving
		, bool_or(is_difficulty_adjustment) as has_difficulty_adjustment
		, count(distinct block_key) as block_count
		, min(blockheight) as blockheight_first
		, max(blockheight) as blockheight_last
		, count(distinct pool_key) as pool_count
		, sum(reward_subsidy) as reward_subsidy_sum
		, sum(reward_tx_fee_sum) as reward_tx_fee_sum
		, sum(tx_count) as tx_count
		, max(audit_created_timestamp) as audit_created_timestamp
	from deduplicate
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
	from aggregate as a
	-- Join to first & last block on each date for difficulty of the last block
	inner join deduplicate as d1
		on d1.blockheight = a.blockheight_first
	inner join deduplicate as d2
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
		from aggregate as a
		-- Join to the first block of each date and the difficulty adjustment block
		inner join deduplicate as d1
			on d1.blockheight = a.blockheight_first
		inner join deduplicate as d2
			on d2.event_date = a.event_date
		where a.has_difficulty_adjustment = true
		and d2.is_difficulty_adjustment = true
	) as da
		on da.event_date = a.event_date
)

, relations as (
	select
		  dd.date_id
		, a.has_subsidy_halving
		, a.has_difficulty_adjustment
		, hd.difficulty_first
		, hd.difficulty_last
		-- If no adjustment occured, fill the weighted avg with the consistent difficulty for those days
		, coalesce(hd.difficulty_weighted_avg, hd.difficulty_first) as difficulty_weighted_avg
		, a.block_count
		, a.blockheight_first
		, a.blockheight_last
		, a.pool_count
		, a.reward_subsidy_sum
		, a.reward_tx_fee_sum
		, a.tx_count
		, a.audit_created_timestamp
	from aggregate as a
	inner join handle_difficulty_adjustments as hd
		on hd.event_date = a.event_date
	inner join dim.date as dd
		on dd.date = a.event_date
)

, standardize as (
	select
		  date_id::varchar(512) as date_id
		, has_subsidy_halving::boolean as has_subsidy_halving
		, has_difficulty_adjustment::boolean as has_difficulty_adjustment
		, difficulty_first::decimal(30, 9) as difficulty_first
		, difficulty_last::decimal(30, 9) as difficulty_last
		, difficulty_weighted_avg::decimal(30, 9) as difficulty_weighted_avg
		, block_count::int as block_count
		, blockheight_first::int as blockheight_first
		, blockheight_last::int as blockheight_last
		, pool_count::int as pool_count
		-- Estimate network hashrate as block count * difficulty * 2^32 / seconds in a day
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
		, (reward_subsidy_sum + reward_tx_fee_sum)::bigint as reward_mining
		, reward_subsidy_sum::bigint as reward_subsidy_sum
		, reward_tx_fee_sum::bigint as reward_tx_fee_sum
		, tx_count::int as tx_count
		-- Add BTC scale numbers (vs Sats scale)
		, (100 * reward_tx_fee_sum::decimal / (reward_subsidy_sum + reward_tx_fee_sum)::decimal
			)::decimal(12, 9) as reward_tx_fee_pct
		, ((reward_subsidy_sum + reward_tx_fee_sum)::decimal / pow(10, 8)::decimal
			)::decimal(16, 8) as reward_mining_btc
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
from fact.network_stats_1d
order by date_id
limit 1000
;
