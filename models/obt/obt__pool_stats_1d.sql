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
	, ps.tx_fee_avg
	, ps.tx_fee_avg_btc
from fact.pool_stats_1d as ps
-- Having coalesced to the default/unknown dim ids in facts, optimal inner joins can be used in obts
inner join dim.date as dd
	on dd.date_id = ps.date_id
inner join dim.pool as dp
	on dp.pool_id = ps.pool_id

);

select *
from obt.pool_stats_1d
order by event_date, block_count desc, pool_name
limit 1000
;
