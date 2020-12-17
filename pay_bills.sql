-- Number os companies that issue at least one bill per month and number of companies that marked bill as paid;
with count_per_event as (
select 
a.company_id,
b.qbo_country,
a.tx_date as create_date,
att.QBO_CHANNEL_SUPER_AGGR_NAME_aux AS QBO_CHANNEL_SUPER_AGGR_NAME,
(case when a.action_type_id = 3 and a.list_type_id = 7 and a.tx_type_id = 9 then 1 else 0 end) as added_bill,
(case when a.action_type_id = 3 and a.list_type_id = 7 and a.tx_type_id in (14,15) then 1 else 0 end) as marked_bill_paid
from SBG_SOURCE.src_qbo_combined_auditinfo_vw a
left join sbg_stable.qbo_company_status b on a.company_id = b.qbo_company_id
left join SBG_PUBLISHED.qbo_company_attributes_ftu as att on att.company_id = a.company_id
where b.qbo_country = 'Brazil' and year(a.tx_date) = 2020 )

select 
year(create_date) as year,
month(create_date) as month,
QBO_CHANNEL_SUPER_AGGR_NAME,
count(DISTINCT case when added_bill = 1 then company_id else 0 end) as added_bill_count,
count(DISTINCT case when marked_bill_paid = 1 then company_id else 0 end) as marked_bill_paid_count,
sum(marked_bill_paid) as total_bills_paid
from count_per_event
group by year(create_date),month(create_date),QBO_CHANNEL_SUPER_AGGR_NAME
order by year(create_date),month(create_date),QBO_CHANNEL_SUPER_AGGR_NAME;

-- Companies within first signup (30days) that created at least one bill;

with signup_first_bill as (
select 
a.qbo_country,
a.qbo_company_id, 
a.qbo_company_current_status,
a.qbo_signup_date,
min(b.tx_date) as first_bill_created,
(case when datediff(day,a.qbo_signup_date,min(b.tx_date)) <=30 then 1 else 0 end) as added_bill_first_30_days
from sbg_stable.qbo_company_status a 
left join SBG_SOURCE.src_qbo_combined_auditinfo_vw b on a.qbo_company_id=b.company_id and b.action_type_id = 3 and b.list_type_id = 7 and b.tx_type_id = 9
where a.qbo_country = 'Brazil' and year(a.qbo_signup_date) = 2020
group by a.qbo_country, a.qbo_company_id,a.qbo_company_current_status,a.qbo_signup_date)

select
year(b.qbo_signup_date) as year,
month(b.qbo_signup_date) as month,
b.qbo_company_current_status,
count (distinct case when b.added_bill_first_30_days = 1 then b.qbo_company_id else null end) total_companies_adding_bills_first30days,
count (distinct b.qbo_company_id)
from signup_first_bill b
group by year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status
order by year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status;

-- Companies within first signup (30days) that created at least one bill - UK and FR;

with signup_first_bill as (
select 
a.qbo_country,
a.qbo_company_id, 
a.qbo_company_current_status,
a.qbo_signup_date,
min(b.tx_date) as first_bill_created,
(case when datediff(day,a.qbo_signup_date,min(b.tx_date)) <=30 then 1 else 0 end) as added_bill_first_30_days
from sbg_stable.qbo_company_status a 
left join SBG_SOURCE.src_qbo_combined_auditinfo_vw b on a.qbo_company_id=b.company_id and b.action_type_id = 3 and b.list_type_id = 7 and b.tx_type_id = 9
where a.qbo_country in ( 'Brazil' , 'France' , 'United Kingdom' ) and year(a.qbo_signup_date) = 2020
group by a.qbo_country, a.qbo_company_id,a.qbo_company_current_status,a.qbo_signup_date)

select
b.qbo_country,
year(b.qbo_signup_date) as year,
month(b.qbo_signup_date) as month,
b.qbo_company_current_status,
count (distinct case when b.added_bill_first_30_days = 1 then b.qbo_company_id else null end) total_companies_adding_bills_first30days,
count (distinct b.qbo_company_id)
from signup_first_bill b
group by b.qbo_country, year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status
order by b.qbo_country, year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status;


-- Companies within first signup (30days)
with signup_first_bill as (
select 
a.qbo_country,
a.qbo_company_id, 
a.qbo_company_current_status,
a.qbo_signup_date,
min(b.tx_date) as first_bill_created,
(case when datediff(day,a.qbo_signup_date,min(b.tx_date)) <=30 then 1 else 0 end) as added_bill_first_30_days
from sbg_stable.qbo_company_status a 
left join SBG_SOURCE.src_qbo_combined_auditinfo_vw b on a.qbo_company_id=b.company_id and b.action_type_id = 3 and b.list_type_id = 7 and b.tx_type_id = 9
where a.qbo_country = 'Brazil' and year(a.qbo_signup_date) = 2020
group by a.qbo_country, a.qbo_company_id,a.qbo_company_current_status,a.qbo_signup_date)

select
year(b.qbo_signup_date) as year,
month(b.qbo_signup_date) as month,
b.qbo_company_current_status,
count (distinct b.qbo_company_id) as total_companies_signups
from signup_first_bill b
group by year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status
order by year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status

-- Companies within first signup (30days) - UK and FR
with signup_first_bill as (
select 
a.qbo_country,
a.qbo_company_id, 
a.qbo_company_current_status,
a.qbo_signup_date,
min(b.tx_date) as first_bill_created,
(case when datediff(day,a.qbo_signup_date,min(b.tx_date)) <=30 then 1 else 0 end) as added_bill_first_30_days
from sbg_stable.qbo_company_status a 
left join SBG_SOURCE.src_qbo_combined_auditinfo_vw b on a.qbo_company_id=b.company_id and b.action_type_id = 3 and b.list_type_id = 7 and b.tx_type_id = 9
where a.qbo_country in ( 'Brazil', 'France' , 'United Kingdom' ) and year(a.qbo_signup_date) = 2020
group by a.qbo_country, a.qbo_company_id,a.qbo_company_current_status,a.qbo_signup_date)

select
b.qbo_country,
year(b.qbo_signup_date) as year,
month(b.qbo_signup_date) as month,
b.qbo_company_current_status,
count (distinct b.qbo_company_id) as total_companies_signups
from signup_first_bill b
group by b.qbo_country, year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status
order by b.qbo_country, year(b.qbo_signup_date), month(b.qbo_signup_date), b.qbo_company_current_status;







-- % Bills paid on time

with bills_paid_on_time as (
select 
b.qbo_country, 
txd.company_id, 
txd.tx_id, 
concat(concat(txd.company_id::varchar,  '-'), txd.tx_id::varchar) as concat_key, 
txd.tx_date, 
txd.tx_type_id, 
txd.due_date, 
txd.paid_date,
att.QBO_CHANNEL_SUPER_AGGR_NAME_aux AS QBO_CHANNEL_SUPER_AGGR_NAME,
(case when txd.due_date>=txd.paid_date then 1 else 0 end) as paid_bill_on_time
from SBG_SOURCE.src_qbo_combined_txdetails_vw as txd
left join sbg_stable.qbo_company_status b on txd.company_id = b.qbo_company_id
left join SBG_PUBLISHED.qbo_company_attributes_ftu as att on att.company_id = txd.company_id
where b.qbo_country = 'Brazil' and txd.tx_type_id = 9 and year(txd.tx_date) = 2020 )

select 
year(a.tx_date) as year,
month(a.tx_date) as month,
a.QBO_CHANNEL_SUPER_AGGR_NAME,
count(distinct a.concat_key) as total_bills,
count (distinct case when a.paid_bill_on_time = 1 then a.concat_key else null end) total_bills_paid_on_time
from bills_paid_on_time a
group by year(a.tx_date), month(a.tx_date), a.QBO_CHANNEL_SUPER_AGGR_NAME
order by year(a.tx_date), month(a.tx_date), a.QBO_CHANNEL_SUPER_AGGR_NAME;
