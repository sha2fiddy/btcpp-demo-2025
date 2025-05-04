set search_path to dim;

drop table if exists date;

create table date (
      date_id varchar(8) not null
    , date date not null
    , epoch_timestamp bigint not null
    , day_of_week int not null
    , day_of_week_name varchar(9) not null
    , day_of_week_name_abbr varchar(3) not null
    , day_of_month int not null
    , day_of_month_suffix varchar(4) not null
    , day_of_quarter int not null
    , day_of_year int not null
    , week_of_month int not null
    , week_of_year int not null
    , week_of_year_iso char(10) not null
    , month int not null
    , month_name varchar(9) not null
    , month_name_abbr char(3) not null
    , quarter int not null
    , quarter_name varchar(9) not null
    , year int not null
    , first_day_of_week date not null
    , last_day_of_week date not null
    , first_day_of_month date not null
    , last_day_of_month date not null
    , first_day_of_quarter date not null
    , last_day_of_quarter date not null
    , first_day_of_year date not null
    , last_day_of_year date not null
    , is_weekend boolean not null
);

alter table date add constraint date_date_id_pk primary key (date_id)
;

create index date_date_idx on date(date)
;

commit
;

insert into date
select
      to_char(gen_date, 'yyyymmdd') as date_id
    , gen_date as date
    , extract(epoch from gen_date) as epoch_timestamp
    , extract(isodow from gen_date) as day_of_week
    , initcap(to_char(gen_date, 'day')) as day_of_week_name
    , initcap(left(to_char(gen_date, 'day'), 3)) as day_of_week_name_abbr
    , extract(day from gen_date) as day_of_month
    , to_char(gen_date, 'fmddth') as day_of_month_suffix
    , gen_date - date_trunc('quarter', gen_date)::date + 1 as day_of_quarter
    , extract(doy from gen_date) as day_of_year
    , to_char(gen_date, 'w')::int as week_of_month
    , extract(week from gen_date) as week_of_year
    , extract(isoyear from gen_date) || to_char(gen_date, '"-w"iw-') || extract(isodow from gen_date) as week_of_year_iso
    , extract(month from gen_date) as month
    , initcap(to_char(gen_date, 'month')) as month_name
    , initcap(left(to_char(gen_date, 'month'), 3)) as month_name_abbr
    , extract(quarter from gen_date) as quarter
    , case
        when extract(quarter from gen_date) = 1 then 'Q1'
        when extract(quarter from gen_date) = 2 then 'Q2'
        when extract(quarter from gen_date) = 3 then 'Q3'
        when extract(quarter from gen_date) = 4 then 'Q4'
    end as quarter_name
    , extract(isoyear from gen_date) as year
    , gen_date + (1 - extract(isodow from gen_date))::int as first_day_of_week
    , gen_date + (7 - extract(isodow from gen_date))::int as last_day_of_week
    , gen_date + (1 - extract(day from gen_date))::int as first_day_of_month
    , (date_trunc('month', gen_date) + interval '1 month - 1 day')::date as last_day_of_month
    , date_trunc('quarter', gen_date)::date as first_day_of_quarter
    , (date_trunc('quarter', gen_date) + interval '3 month - 1 day')::date as last_day_of_quarter
    , to_date(extract(year from gen_date) || '-01-01', 'yyyy-mm-dd') as first_day_of_year
    , to_date(extract(year from gen_date) || '-12-31', 'yyyy-mm-dd') as last_day_of_year
    , case
        when extract(isodow from gen_date) in (6, 7) then true
        else false
    end as is_weekend
from (
    select '1970-01-01'::date + sequence.day as gen_date
    from generate_series(0, 29219) as sequence (day)
    group by sequence.day
)
order by 1
;

commit
;
