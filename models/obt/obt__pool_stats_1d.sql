drop view if exists obt.pool_stats_1d cascade
;

create view obt.pool_stats_1d as (

-- Using a logical column order will help consumers
select
	  dd.date_id
	, dd.date as event_date
	-- Adding every col from dim date is probably not needed in most cases
	, dd.day_of_week
	, dd.day_of_month
	, dd.day_of_year
	, fp.has_subsidy_halving
	, fp.has_difficulty_adjustment
	, fp.difficulty_first
	, fp.difficulty_last
	, fp.difficulty_weighted_avg
	, dp.pool_id
	, dp.pool_key
	, dp.pool_name
	, dp.pool_url
	, dp.is_antpool_friend_custodian
	, dp.is_antpool_friend_template
	, fp.reported_hashrate
	, fp.reported_hashrate_th
	, fp.reported_hashrate_ph
	, fp.reported_hashrate_eh
	, fp.est_hashrate
	, fp.est_hashrate_th
	, fp.est_hashrate_ph
	, fp.est_hashrate_eh
	, fp.block_count
	, fp.blockheight_first
	, fp.blockheight_last
	, fp.reward_mining_sum
	, fp.reward_subsidy_sum
	, fp.reward_tx_fee_sum
	, fp.tx_count
	, fp.reward_tx_fee_pct
	, fp.reward_mining_sum_btc
	, fp.reward_subsidy_sum_btc
	, fp.reward_tx_fee_sum_btc
	, fp.tx_fee_avg
	, fp.tx_fee_avg_btc
from fact.pool_stats_1d as fp
-- Having coalesced to the default/unknown dim ids in facts, optimal inner joins can be used in obts
inner join dim.date as dd
	on dd.date_id = fp.date_id
inner join dim.pool as dp
	on dp.pool_id = fp.pool_id

);

select *
from obt.pool_stats_1d
order by event_date, block_count desc, pool_name
limit 1000
;
