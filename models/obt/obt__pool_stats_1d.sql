drop view if exists obt.pool_stats_1d cascade
;

create view obt.pool_stats_1d as (


with combined_data as (
	-- Using a logical column order will help consumers
	select
		  dd.date_id
		, dd.date as event_date
		-- Adding every col from dim date is probably not needed in most cases
		, dd.day_of_week
		, dd.day_of_month
		, dd.day_of_year
		, dp.pool_id
		, dp.pool_key
		, dp.pool_name
		, dp.pool_url
		, dp.is_antpool_friend
		, dp.is_antpool_friend_custodian
		, dp.is_antpool_friend_template
		, ps.reported_hashrate
		, ps.reported_hashrate_th
		, ps.reported_hashrate_ph
		, ps.reported_hashrate_eh
		, ps.est_hashrate
		, ps.est_hashrate_th
		, ps.est_hashrate_ph
		, ps.est_hashrate_eh
		-- Derive expected block count as hashate * seconds in a day / difficulty / 2^32
		, (ps.reported_hashrate * 86400 / ns.difficulty_weighted_avg / pow(2, 32)
			)::decimal(12, 9) as expected_block_count
		, ps.block_count
		, ps.blockheight_first
		, ps.blockheight_last
		, ps.reward_mining_sum
		, ps.reward_subsidy_sum
		, ps.reward_tx_fee_sum
		, ps.tx_count
		, ps.reward_tx_fee_pct
		, ps.reward_mining_sum_btc
		, ps.reward_subsidy_sum_btc
		, ps.reward_tx_fee_sum_btc
		-- Derive USD amounts based on daily price close
		, (p.price_close * ps.reward_mining_sum_btc)::decimal(21, 6) as reward_mining_sum_usd
		, (p.price_close * ps.reward_subsidy_sum_btc)::decimal(21, 6) as reward_subsidy_sum_usd
		, (p.price_close * ps.reward_tx_fee_sum_btc)::decimal(21, 6) as reward_tx_fee_sum_usd
		, ps.tx_fee_avg
		, ps.tx_fee_avg_btc
		, (p.price_close * ps.tx_fee_avg_btc)::decimal(21, 6) as tx_fee_avg_usd
	from fact.pool_stats_1d as ps
	-- Having coalesced to the default/unknown dim ids in facts, optimal inner joins can be used in obts
	inner join fact.network_stats_1d as ns
		on ns.date_id = ps.date_id
	inner join fact.price_1d as p
		on p.date_id = ps.date_id
	inner join dim.date as dd
		on dd.date_id = ps.date_id
	inner join dim.pool as dp
		on dp.pool_id = ps.pool_id
)

select
	  date_id
	, event_date
	, day_of_week
	, day_of_month
	, day_of_year
	, pool_id
	, pool_key
	, pool_name
	, pool_url
	, is_antpool_friend
	, is_antpool_friend_custodian
	, is_antpool_friend_template
	, reported_hashrate
	, reported_hashrate_th
	, reported_hashrate_ph
	, reported_hashrate_eh
	, est_hashrate
	, est_hashrate_th
	, est_hashrate_ph
	, est_hashrate_eh
	, expected_block_count
	, block_count
	-- Derive actual vs expected block count diff, pct diff, and mining 'luck' (aka variability)
	, (block_count::decimal - expected_block_count)::decimal(12, 9) as block_count_diff
	, ((100::decimal * block_count::decimal - expected_block_count) / expected_block_count
		)::decimal(12, 9) as block_count_diff_pct
	, ((100::decimal * block_count::decimal) / expected_block_count)::decimal(12, 9) as mining_luck
	, blockheight_first
	, blockheight_last
	, reward_mining_sum
	, reward_subsidy_sum
	, reward_tx_fee_sum
	, tx_count
	, reward_tx_fee_pct
	, reward_mining_sum_btc
	, reward_subsidy_sum_btc
	, reward_tx_fee_sum_btc
	, reward_mining_sum_usd
	, reward_subsidy_sum_usd
	, reward_tx_fee_sum_usd
	, tx_fee_avg
	, tx_fee_avg_btc
	, tx_fee_avg_usd
from combined_data

);

select *
from obt.pool_stats_1d
order by event_date, block_count desc, pool_name
limit 1000
;
