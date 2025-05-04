drop view if exists obt.network_stats_1d cascade
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
	, p.ticker
	, p.price_close
	, ns.has_subsidy_halving
	, ns.has_difficulty_adjustment
	, ns.difficulty_first
	, ns.difficulty_last
	, ns.difficulty_weighted_avg
	, ns.block_count
	, ns.blockheight_first
	, ns.blockheight_last
	, ns.pool_count
	, ns.est_hashrate
	, ns.est_hashrate_th
	, ns.est_hashrate_ph
	, ns.est_hashrate_eh
	, ns.reward_mining_sum
	, ns.reward_subsidy_sum
	, ns.reward_tx_fee_sum
	, ns.tx_count
	, ns.reward_tx_fee_pct
	, ns.reward_mining_sum_btc
	, ns.reward_subsidy_sum_btc
	, ns.reward_tx_fee_sum_btc
	-- Derive USD amounts based on daily price close
	, (p.price_close * reward_mining_sum_btc)::decimal(21, 6) as reward_mining_sum_usd
	, (p.price_close * reward_subsidy_sum_btc)::decimal(21, 6) as reward_subsidy_sum_usd
	, (p.price_close * reward_tx_fee_sum_btc)::decimal(21, 6) as reward_tx_fee_sum_usd
	, ns.tx_fee_avg
	, ns.tx_fee_avg_btc
	, (p.price_close * tx_fee_avg_btc)::decimal(21, 6) as tx_fee_avg_usd
from fact.network_stats_1d as ns
-- Having coalesced to the default/unknown dim ids in facts, optimal inner joins can be used in obts
inner join fact.price_1d as p
	on p.date_id = ns.date_id
inner join dim.date as dd
	on dd.date_id = ns.date_id

);

select *
from obt.network_stats_1d
order by event_date
limit 1000
;
