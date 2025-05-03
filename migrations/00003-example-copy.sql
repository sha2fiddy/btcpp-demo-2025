set search_path to src;

drop table if exists example_user;

create table example_user (
    id int primary key,
    name text,
    age int
);

-- Load data from csv
copy example_user (id, name, age)
from '/data/example_user.csv'
delimiter ','
csv header;
