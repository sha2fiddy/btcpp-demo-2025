set search_path to src;

drop table if exists block;

create table block (
      blockheight int not null
    , block_hash text not null
    , prev_block_hash text not null
    , timestamp timestamp not null
    , pool_key text
    , difficulty decimal(30, 7)
    , reward_subsidy bigint
    , reward_tx_fee_sum bigint
    , tx_count int
);

copy block (
      blockheight
    , block_hash
    , prev_block_hash
    , timestamp
    , pool_key
    , difficulty
    , reward_subsidy
    , reward_tx_fee_sum
    , tx_count
)

from '/data/block.csv'
delimiter ','
csv header;
