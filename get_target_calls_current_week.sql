SET DATEFIRST 1;

with

dates as (
	SELECT
	id as date_id,
		simple_date
	FROM general_dates
	where simple_date 
		between dateadd(day, -(datepart(weekday, dateadd(day, -1, convert(date, GETDATE())))), dateadd(day, 0, convert(date, GETDATE())))
		and dateadd(day, 0, convert(date, GETDATE()))
	group by simple_date, id
),

ct_calls as (
	SELECT 
		calls_id,
		dates_id,
		traffic_id
	from calltouch_calls_facts
	where account_id = 10534
	group by calls_id, dates_id, traffic_id
),

traffic_sources as (
	SELECT 
		id,
		concat(source, ' / ', medium) as source_medium
	from general_traffic
	where concat(source, ' / ', medium) in ('yandex_tm / cpc', 'mytarget_tm / cpc', '{source}')
	group by id, source, medium 
),

phones as (
	select 
		id,
		client_phone
	FROM calltouch_calls
	group by id, client_phone, callback_call
	),

tags as (
	select
		calls_id,
		tag_type,
		name
	from calltouch_calls_tags
	where name like '{tag}'
	group by id, tag_type , category, name , calls_id
),

join_table as (
	SELECT 
		dates.simple_date,
		
		traffic_sources.source_medium,
		
		phones.client_phone,
		
		tags.calls_id,
		tags.name
	from dates
	left join ct_calls
		on dates.date_id = ct_calls.dates_id
	left join traffic_sources
		on ct_calls.traffic_id = traffic_sources.id
	left join phones
		on phones.id = ct_calls.calls_id
	left join tags
		on phones.id = tags.calls_id
)

select
	count(distinct client_phone) as all_phone
from join_table
where source_medium in ('yandex_tm / cpc', 'mytarget_tm / cpc', '{source}')
	and name is not null

