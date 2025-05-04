set search_path to src
;

drop table if exists hashrate
;

create table hashrate (
      date date not null
    , pool_key text not null
    , hashrate decimal(30, 0)
);

copy hashrate (
      date
    , pool_key
    , hashrate
)

from '/data/hashrate.csv'
delimiter ','
csv header
;
