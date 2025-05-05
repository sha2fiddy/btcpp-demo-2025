drop table if exists fact.price_1d cascade
;

create table fact.price_1d as (

with src_price as (
	select distinct
		-- Apply some basic data cleaning to string fields (good place to use a dbt macro)
		  date::date as event_date
		-- If dealing with more than one coin or ticker, a separate dim coin and coin key/id are needed
		, trim(ticker::varchar) as ticker
		, price_open::decimal(21, 2) as price_open
	    , price_close::decimal(21, 2) as price_close
	    , price_low::decimal(21, 2) as price_low
	    , price_high::decimal(21, 2) as price_high
		-- Create a dummy timestamp for deduplication (ideally there is a real audit timestamp from src data)
		, current_timestamp as audit_created_timestamp
	from src.price
	-- Hard coded filter for price ticker, for simplicity
	where trim(ticker::varchar) = 'BTC-USD'
	-- Typically fact tables would build incrementally on some schedule, and a date filter would be applied accordingly
)

-- Remove any duplicate data that could exist in src layer
, deduplicate as (
	select *
	from (
		select
			  *
			, row_number() over (
				-- The partition defines the data granularity (unique natural key or keys)
				partition by event_date
				-- This should be a timestamp of when source data was last updated (lacking that, when row was loaded to src table)
				order by audit_created_timestamp desc
			) as dedupe_rn
		from src_price
	)
	where dedupe_rn = 1
)

, relations as (
	select
		  dd.date_id
		-- Including a categorical field directly in a fact is called a 'degenerate dimension'
		, d.ticker
		, d.price_open
		, d.price_close
		, d.price_low
		, d.price_high
	from deduplicate as d
	inner join dim.date as dd
		on dd.date = d.event_date
)

-- Specify final data types, add derived cols
, standardize as (
	select
		  date_id::varchar(512) as date_id
		, ticker::varchar(10) as ticker
		, price_open::decimal(21, 2) as price_open
		, price_close::decimal(21, 2) as price_close
		, (price_close - price_open)::decimal(21, 2) as price_change
		, (case
			when price_open = 0 then null
			else 100::decimal * (price_close - price_open)::decimal / price_open::decimal
		end)::decimal(18, 9) as price_change_pct
		, price_low::decimal(21, 2) as price_low
		, price_high::decimal(21, 2) as price_high
		, (price_high - price_low)::decimal(21, 2) as price_spread
	from relations
)

select *
from standardize

);

select *
from fact.price_1d
order by date_id
limit 1000
;
