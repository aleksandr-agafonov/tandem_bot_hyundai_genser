with

yesterday as (
	SELECT
		id,
		simple_date
	FROM general_dates
	where simple_date 
		between dateadd(month, DATEDIFF(month, 0, dateadd(Month, -1, eomonth(GETDATE()))), 0)
		and dateadd(Month, -1, eomonth(GETDATE()))
	group by id, simple_date

),

yd_fact as (
	SELECT 
		dates_id,
		sum(cost) as yd_cost
	from direct_campaigns_facts
	where account_id = 16924
	group by dates_id 
),

ad_fact as (
	SELECT 
		dates_id,
		sum(cost) as ad_cost
	from adwords_campaigns_facts
	where account_id = 30156
	group by dates_id 
),

mt_facts as (
	SELECT 
		dates_id,
		sum(cost) * 1.2 as mt_cost
	from mytarget_campaigns_facts
	where account_id = 33269
	group by dates_id 
),

join_table as (
	select
		yesterday.simple_date,
		
		CASE 
			when yd_fact.yd_cost is not null
				then yd_fact.yd_cost
			else 0
		end as yd_cost,
		
		CASE 
			when ad_fact.ad_cost is not null
				then ad_fact.ad_cost
			else 0
		end as ad_cost,
		
		CASE 
			when mt_facts.mt_cost  is not null
				then mt_facts.mt_cost
			else 0
		end as mt_cost
		
	from yesterday
	left join yd_fact
		on yesterday.id = yd_fact.dates_id
	left join ad_fact
		on yesterday.id = ad_fact.dates_id
	left join mt_facts
		on yesterday.id = mt_facts.dates_id	
)

select
	(sum(yd_cost) + sum(ad_cost) + sum(mt_cost)) as total_adcost
from join_table
