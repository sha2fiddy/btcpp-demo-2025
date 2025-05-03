set search_path to src;

drop table if exists example_random;

create table example_random (
    id bigint generated always as identity,
    primary key(id),
    hash_a text not null,
    hash_b text not null,
    bool_a boolean not null,
    bool_b boolean not null
);

-- Load random data
insert into example_random (hash_a, hash_b, bool_a, bool_b)
select
    md5(random()::text),
    md5(random()::text),
    case when random() < 0.5 then true else false end,
    case when random() < 0.5 then true else false end
from generate_series(1, 10000);
