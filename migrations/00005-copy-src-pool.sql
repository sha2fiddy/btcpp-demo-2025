set search_path to src;

drop table if exists pool;

create table pool (
      pool_key text not null
    , pool_name text not null
    , pool_url text
    , is_antpool_friend_custodian boolean
    , is_antpool_friend_template boolean
);

copy pool (
      pool_key
    , pool_name
    , pool_url
    , is_antpool_friend_custodian
    , is_antpool_friend_template
)

from '/data/pool.csv'
delimiter ','
csv header;
