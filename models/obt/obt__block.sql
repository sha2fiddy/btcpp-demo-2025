-- OBTs could be tables or materialized views if performance is an issue
drop view if exists obt.block
;

create view obt.block as (

-- Using a logical column order will help consumers
select
	  db.block_id
	, db.block_key
	, db.block_hash
	, fb.blockheight
	, db.is_subsidy_halving
	, fb.subsidy_epoch
	, fb.subsidy_level
	, fb.subsidy_level_btc
	, db.is_difficulty_adjustment
	, fb.difficulty_epoch
	, fb.difficulty
	, dd.date_id
	, dd.date as event_date
	, fb.event_timestamp
	-- Adding every col from dim date is probably not needed in most cases
	, dd.day_of_week
	, dd.day_of_month
	, dd.day_of_year
	, dp.pool_id
	, dp.pool_key
	, dp.pool_name
	, dp.pool_url
	, dp.is_antpool_friend_custodian
	, dp.is_antpool_friend_template
	, fb.reward_mining
	, fb.reward_subsidy
	, fb.reward_tx_fee_sum
	, fb.reward_tx_fee_pct
	, fb.reward_mining_btc
	, fb.reward_subsidy_btc
	, fb.reward_tx_fee_sum_btc
	, fb.tx_count
	, fb.tx_fee_avg
	, fb.tx_fee_avg_btc
from fact.block as fb
-- Having coalesced to the default/unknown dim ids in facts, optimal inner joins can be used in obts
inner join dim.block as db
	on db.block_id = fb.block_id
inner join dim.date as dd
	on dd.date_id = fb.date_id
inner join dim.pool as dp
	on dp.pool_id = fb.pool_id

);

select *
from obt.block
order by blockheight
limit 1000
;
