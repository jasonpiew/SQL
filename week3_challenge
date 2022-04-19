How many customers has Foodie-Fi ever had?
What is the monthly distribution of trial plan start_date values for our dataset - use the start of the month as the group by value
What plan start_date values occur after the year 2020 for our dataset? Show the breakdown by count of events for each plan_name
What is the customer count and percentage of customers who have churned rounded to 1 decimal place?
How many customers have churned straight after their initial free trial - what percentage is this rounded to the nearest whole number?
What is the number and percentage of customer plans after their initial free trial?
What is the customer count and percentage breakdown of all 5 plan_name values at 2020-12-31?
How many customers have upgraded to an annual plan in 2020?
How many days on average does it take for a customer to an annual plan from the day they join Foodie-Fi?
Can you further breakdown this average value into 30 day periods (i.e. 0-30 days, 31-60 days etc)
How many customers downgraded from a pro monthly to a basic monthly plan in 2020?

------------------------------------
1--How many customers has Foodie-Fi ever had?
select count(distinct s.customer_id) as totalcustomerunique
from subscriptions as s

--What is the monthly distribution of trial plan start_date values for our dataset - use the start of the month as the group by value

2--extracted month and year 
with extracted as(
select
extract(year from start_date) as yy,
to_char(start_date, 'MM') as mm,
to_char(start_date, 'Month') as mm_name,
customer_id as trialplan
from subscriptions as s
where plan_id = 0
)
select yy,mm,mm_name,count(trialplan)
from extracted
group by yy,mm,mm_name
order by yy asc, mm asc

3--What plan start_date values occur after the year 2020 for our dataset? Show the breakdown by count of events for each plan_name
--same as above, extract year only and no filter for the plan
with extracted as(
select
extract(year from start_date) as yy,
s.plan_id, p.plan_name, customer_id as total 
from subscriptions as s
join "plans" as p 
on s.plan_id = p.plan_id 
)
,extracted_and_summed as(
select yy,plan_id,plan_name,count(total) as totalplan
from extracted
group by 1,2,3
)
,extracted_and_summed_partitioned as (
select *, sum(totalplan)over(partition by yy) as totalperyear
from extracted_and_summed
)
select *, round(totalplan::float/totalperyear::float*100) as percentagediffer
from extracted_and_summed_partitioned;

4--What is the customer count and percentage of customers who have churned rounded to 1 decimal place?
with customer_that_churned as(
select customer_id, s.plan_id, p.plan_name 
from subscriptions as s 
join "plans" as p 
on s.plan_id  = p.plan_id
where s.plan_id = 4
)
,customer_that_churned_summed as(
select plan_id,plan_name,count(*) as customerthatchurned
from customer_that_churned
group by 1,2
)
select *, (select count(distinct customer_id)from subscriptions) as totalcustomer,
100*customerthatchurned::numeric/(select count(distinct customer_id)from subscriptions) as percentage 
from customer_that_churned_summed; 

5--How many customers have churned straight after their initial free trial - what percentage is this rounded to the nearest whole number?

--using lead to find customer that churned after free trial
with customer_churned_afterfreetrial as (
select customer_id, p.plan_name, lead(p.plan_name,1)over(partition by customer_id order by start_date) as nextplan 
from subscriptions as s 
join "plans" as p 
on s.plan_id = p.plan_id 
)
--filter customer that churned after trial
select count(distinct customer_id) as trial_then_churn,
count(distinct customer_id)::numeric/(select count(distinct customer_id)from subscriptions)*100 as percentagechurn
from customer_churned_afterfreetrial
where plan_name = 'trial' and nextplan = 'churn'

6--What is the number and percentage of customer plans after their initial free trial?
with customernextplan as (
select customer_id, p.plan_name, lead(p.plan_name,1)over(partition by customer_id order by start_date) as nextplan 
from subscriptions as s 
join "plans" as p 
on s.plan_id = p.plan_id 
)
select nextplan as nextplanaftertrial, count(distinct customer_id) as totalplancount,
100*count(distinct customer_id)::numeric/(select count(distinct customer_id)from subscriptions) as percentagechurn
from customernextplan
where plan_name = 'trial'
group by 1

7--What is the customer count and percentage breakdown of all 5 plan_name values at 2020-12-31? (LEARN AGAIN)
with customerplanandyearfilter as (
select customer_id, s.plan_id, p.plan_name, s.start_date,
lead(s.plan_id,1)over(partition by customer_id order by start_date) as startnextplan
from subscriptions as s
join "plans" as p 
on s.plan_id = p.plan_id 
where extract(YEAR from start_date)=2020
)
,customerplanandyearfilter_summed as(
select count(customer_id) as totalofplan, plan_id
from customerplanandyearfilter
where startnextplan is null
group by 2)
select plan_id,totalofplan,
100*totalofplan::numeric/(select count(distinct customer_id)from subscriptions) as percentage
from customerplanandyearfilter_summed
group by  1,2

8--How many customers have upgraded to an annual plan in 2020?
select count(distinct customer_id) as annualplancustomer
from subscriptions as s 
where plan_id = 3
and extract(year from start_date) =2020

9--How many days on average does it take for a customer to an annual plan from the day they join Foodie-Fi?()
with extracted_annual as (
select s.customer_id, s.plan_id, s.start_date,
lead(start_date,1)over(partition by customer_id order by start_date) as next_annual
from subscriptions as s
where plan_id =0 or plan_id=3)
select round(avg(next_annual-start_date),2)
from extracted_annual

---method 2
with start_date_trial as(
  select customer_id, start_date
      from subscriptions
      where plan_id = 0)
,annual_date as(
  select customer_id, start_date as annual_date_plan
      from subscriptions
      where plan_id = 3)
select avg(annual_date_plan-start_date) as average
from start_date_trial sdt join annual_date ad 
on sdt.customer_id = ad.customer_id

10--Can you further breakdown this average value into 30 day periods (i.e. 0-30 days, 31-60 days etc)
with start_date_trial as(
  select customer_id, start_date
from subscriptions
where plan_id = 0)
, annual_date as(
  select customer_id, start_date as annual_date_plan
from subscriptions
where plan_id = 3)
, daygap_summed as(
select annual_date_plan-start_date as daygap,ad.customer_id
from start_date_trial sdt
join annual_date ad 
on sdt.customer_id = ad.customer_id
)
select count(customer_id), daydetail
from(
select customer_id,
case when daygap >0 and daygap <= 30 then '0-30 days'
when daygap >=31 and daygap <= 60 then '31-60 days'
when daygap >=61 and daygap <= 90 then '61-90 days'
when daygap >=91 and daygap <= 120 then '91-120 days'
when daygap >=121 then '121 and more days'
end as daydetail
from daygap_summed
) a
group by daydetail\

--method 2
with start_date_trial as(
  select customer_id, start_date as date_trial
from subscriptions
where plan_id = 0)
, annual_date as(
  select customer_id, start_date as annual_date_plan
from subscriptions
where plan_id = 3)
, 
--sort value into 12 bins
daygap_bucket as (
select width_bucket(ad.annual_date_plan - sdt.date_trial,0, 360, 12)  as daygap
from start_date_trial sdt
join annual_date ad 
on sdt.customer_id = ad.customer_id
)
select concat((daygap -1) *30 ,'-',(daygap) * 30, ' days') as timebreakdown,count(*) customercount
from daygap_bucket
group by 1
order by timebreakdown asc

11--How many customers downgraded from a pro monthly to a basic monthly plan in 2020?
with downgraded as (
select *,lead(plan_id,1)over(partition by customer_id order by start_date) as nextplan 
from subscriptions
where extract(year from start_date)= 2020
)
select count(*)
from downgraded
where nextplan = 1
and plan_id = 2
