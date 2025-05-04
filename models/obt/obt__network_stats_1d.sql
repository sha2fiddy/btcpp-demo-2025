drop view if exists obt.network_stats_1d
;

create view obt.network_stats_1d as (

-- Using a logical column order will help consumers
select
	  dd.date_id
	, dd.date as event_date
	-- Adding every col from dim date is probably not needed in most cases
	, dd.day_of_week
	, dd.day_of_month
	, dd.day_of_year
	, fn.has_subsidy_halving
	, fn.has_difficulty_adjustment
	, fn.difficulty_first
	, fn.difficulty_last
	, fn.difficulty_weighted_avg
	, fn.block_count
	, fn.blockheight_first
	, fn.blockheight_last
	, fn.pool_count
	, fn.est_hashrate
	, fn.est_hashrate_th
	, fn.est_hashrate_ph
	, fn.est_hashrate_eh
	, fn.reward_mining
	, fn.reward_subsidy_sum
	, fn.reward_tx_fee_sum
	, fn.tx_count
	, fn.reward_tx_fee_pct
	, fn.reward_mining_btc
	, fn.reward_subsidy_sum_btc
	, fn.reward_tx_fee_sum_btc
	, fn.tx_fee_avg
	, fn.tx_fee_avg_btc
from fact.network_stats_1d as fn
-- Having coalesced to the default/unknown dim ids in facts, optimal inner joins can be used in obts
inner join dim.date as dd
	on dd.date_id = fn.date_id

);

select *
from obt.network_stats_1d
order by event_date
limit 1000
;
