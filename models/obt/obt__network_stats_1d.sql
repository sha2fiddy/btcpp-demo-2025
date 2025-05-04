drop view if exists obt.network_stats_1d cascade
;

create view obt.network_stats_1d as (

with combined_data as (
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
        -- Derive Sats-denominated network-level hashvalue based on estimated hashrate
        , (ns.reward_mining_sum::decimal / est_hashrate_th::decimal)::decimal(30, 9) as hashvalue_sat_th
        , (ns.reward_mining_sum::decimal / est_hashrate_ph::decimal)::decimal(30, 9) as hashvalue_sat_ph
    from fact.network_stats_1d as ns
    -- Having coalesced to the default/unknown dim ids in facts, optimal inner joins can be used in obts
    inner join fact.price_1d as p
        on p.date_id = ns.date_id
    inner join dim.date as dd
        on dd.date_id = ns.date_id
)

select
      date_id
    , event_date
    , day_of_week
    , day_of_month
    , day_of_year
    , ticker
    , price_close
    , has_subsidy_halving
    , has_difficulty_adjustment
    , difficulty_first
    , difficulty_last
    , difficulty_weighted_avg
    , block_count
    , blockheight_first
    , blockheight_last
    , pool_count
    , est_hashrate
    , est_hashrate_th
    , est_hashrate_ph
    , est_hashrate_eh
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
    , hashvalue_sat_th
    , hashvalue_sat_ph
    -- Derive USD-denominated network-level hashprice based on estimated hashprice and daily price close
    , (price_close::decimal / pow(10, 8)::decimal * hashvalue_sat_th)::decimal(30, 9) as hashprice_usd_th
    , (price_close::decimal / pow(10, 8)::decimal * hashvalue_sat_ph)::decimal(30, 9) as hashprice_usd_ph
from combined_data

);

select *
from obt.network_stats_1d
order by event_date
limit 1000
;
