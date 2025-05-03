set search_path to src;

drop table if exists price;

create table price (
      date date not null
    , ticker text not null
    , price_open numeric(21, 2)
    , price_close numeric(21, 2)
    , price_low numeric(21, 2)
    , price_high numeric(21, 2)
);

copy price (
      date
    , ticker
    , price_open
    , price_close
    , price_low
    , price_high
)

from '/data/price.csv'
delimiter ','
csv header;
