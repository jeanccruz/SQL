SET search_path to SBG_PUBLISHED;

DROP TABLE IF EXISTS qbobr_datainmetrics;
CREATE TABLE  qbobr_datainmetrics AS /*+ DIRECT */
SELECT 
    att.country,
    dc.date_for_day,
    dc.week_for_year_fy as week_num,
    dc.week_start_date_fy as week_start,
    dc.month_start_date_fy as month_start,
    month(dc.month_start_date_fy) as month_start_calc,
    dc.quarter_start_date_fy as quarter_start,
    dc.year_fy,
    dc.week_for_year_544 as week_num_544,
    dc.week_start_date_544 as week_start_544,
    dc.year_544,
    att.QBO_CHANNEL_SUPER_AGGR_NAME_aux AS QBO_CHANNEL_SUPER_AGGR_NAME,
    SUM(r.signup) as signups,
    SUM(r.new_subscriber) as gns,
    SUM(CASE WHEN r.new_subscriber = 1 then att.gns_retained_day31 end) as gns_retained_day31, -- State at day 31 ~1 billing cycle
    SUM(CASE WHEN r.new_subscriber = 1 then att.gns_retained_day62 end) as gns_retained_day62, -- State at day 62 ~2 billing cycle
    SUM(CASE WHEN r.new_subscriber = 1 then att.gns_retained_day92 end) as gns_retained_day92, -- State at day 92 ~3 billing cycle
    SUM(CASE WHEN r.new_subscriber = 1 then att.gns_retained_day366 end) as gns_retained_day366, -- State at day 366 (1 full year)
    SUM(r.open_subscriber) as open_subs,
    SUM(ftu.added_txn_first30d) as added_txn_first30d
    
FROM SBG_STABLE.qbo_reporting_daily as r

INNER JOIN SBG_DM.dim_calendar as dc
    on dc.date_for_day = r.date_of 

INNER JOIN SBG_PUBLISHED.qbo_company_attributes_ftu as att
    on att.company_id = r.qbo_company_id 

LEFT JOIN SBG_PUBLISHED.qbo_auditinfo_ftu_metrics as ftu
    on ftu.qbo_company_id = r.qbo_company_id
    AND r.signup=1

WHERE r.date_of>=add_months(date_trunc('month',CURRENT_DATE),-12) and  att.country = 'Brazil' -- past 12 months and only Brazil

GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT ;

ALTER TABLE qbobr_datainmetrics add COLUMN active_customers_past30days INT;
ALTER TABLE qbobr_datainmetrics add COLUMN logged_in_day int;
ALTER TABLE qbobr_datainmetrics add COLUMN logged_in_week int;
ALTER TABLE qbobr_datainmetrics add COLUMN logged_in_month int;
ALTER TABLE qbobr_datainmetrics add COLUMN added_txn_past31d INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_invoice_past31d INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_expense_past31d INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_txn_past31d_mobileapp INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_invoice_past31d_mobileapp INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_expense_past31d_mobileapp INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_txn_past31d_web INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_invoice_past31d_web INT;
ALTER TABLE qbobr_datainmetrics add COLUMN added_expense_past31d_web INT;
ALTER TABLE qbobr_datainmetrics add COLUMN total_txns INT;
ALTER TABLE qbobr_datainmetrics add COLUMN manual_txns INT;
ALTER TABLE qbobr_datainmetrics add COLUMN bank_txns_added INT;
ALTER TABLE qbobr_datainmetrics add COLUMN bank_txns_added_auto_rule INT;

COMMIT ;
-- Active Customers: active_customers_past30days

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics ( country, date_for_day, week_num, week_start, month_start, month_start_calc, quarter_start, year_fy, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME, active_customers_past30days)
with mau as
(
select
  d.qbo_company_id,
  d.date_of,
  d.open_subscriber,
  d.qbo_country,
  max(CASE WHEN ai.company_id IS NULL THEN 0 ELSE 1 END)
    OVER(PARTITION BY d.qbo_company_id ORDER BY d.date_of RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW) as MAU
from sbg_stable.qbo_reporting_daily d
  left join SBG_SOURCE.src_qbo_combined_auditinfo_vw ai on ai.company_id = d.qbo_company_id
    and ai.audit_date = d.date_of
    and ai.action_type_id = 1 -- logins & credentialed app launches
    and ai.user_id > 100      -- filter out system generated events (future-proofing)
where d.open_subscriber = 1
  and d.date_of between add_months(date_trunc('month',CURRENT_DATE),-12) and current_date - 1 -- past 23 months, vertica has truncated data (24 mo)
group by d.qbo_company_id,d.date_of,d.open_subscriber,d.qbo_country,ai.company_id
)
select
  att.country,
  dc.date_for_day,
  dc.week_for_year_fy as week_num,
  dc.week_start_date_fy as week_start,
  dc.month_start_date_fy as month_start,
  month(dc.month_start_date_fy) as month_start_calc,
  dc.quarter_start_date_fy as quarter_start,
  dc.year_fy,
  dc.week_for_year_544 as week_num_544,
  dc.week_start_date_544 as week_start_544,
  dc.year_544,
  att.qbo_channel_super_aggr_name_aux as qbo_channel_super_aggr_name,
  sum(m.mau) as active_customers_past30days
from mau m
  join sbg_stable.qbo_company_status cs on cs.qbo_company_id = m.qbo_company_id
  join sbg_published.qbo_company_attributes_ftu att on att.company_id = cs.qbo_company_id
  join sbg_dm.dim_calendar dc on m.date_of = dc.date_for_day
  join sbg_stable.qbo_reporting_daily r on m.qbo_company_id = r.qbo_company_id and r.date_of = m.date_of
  left join sbg_published.qbo_product_short_list qpsl on r.product_current = qpsl.product
  LEFT JOIN SBG_PUBLISHED.qbo_auditinfo_ftu_metrics as ftu on ftu.qbo_company_id = r.qbo_company_id AND r.signup=1
  
  WHERE r.date_of>=add_months(date_trunc('month',CURRENT_DATE),-12) and  att.country = 'Brazil' -- past 12 months and only Brazil

GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics ( country, date_for_day, week_num, week_start, month_start, month_start_calc, quarter_start, year_fy, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME, logged_in_day)
SELECT
  att.country,
  dc.date_for_day,
  dc.week_for_year_fy as week_num,
  dc.week_start_date_fy as week_start,
  dc.month_start_date_fy as month_start,
  month(dc.month_start_date_fy) as month_start_calc,
  dc.quarter_start_date_fy as quarter_start,
  dc.year_fy,
  dc.week_for_year_544 as week_num_544,
  dc.week_start_date_544 as week_start_544,
  dc.year_544,
  att.qbo_channel_super_aggr_name_aux as qbo_channel_super_aggr_name,
  SUM(case when e.login_excl_first_30d and r.open_subscriber=1 then e.login_excl_first_30d else 0 end) AS logged_in_day

FROM SBG_PUBLISHED.qbo_raw_billing_events_daily as e

INNER JOIN SBG_DM.dim_calendar as dc
    on dc.date_for_day = e.event_date

INNER JOIN SBG_STABLE.qbo_company_status as cs
    on cs.qbo_company_id = e.company_id

INNER JOIN SBG_PUBLISHED.qbo_company_attributes_ftu as att
    on att.company_id = cs.QBO_COMPANY_ID

LEFT JOIN SBG_STABLE.qbo_reporting_daily as r
    on r.qbo_company_id = e.company_id
    and r.date_of = e.event_date

LEFT JOIN sbg_published.qbo_product_short_list qpsl
    on r.product_current = qpsl.product


WHERE e.event_date>=add_months(date_trunc('month',CURRENT_DATE),-12) and  att.country = 'Brazil' -- past 12 months and only Brazil -- past 36 months

GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics ( country, date_for_day, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME, logged_in_week)
SELECT
    att.country,
    dc.week_start_date_544 AS date_for_day,  -- done intentionally,
    dc.week_for_year_544 as week_num_544,
    dc.week_start_date_544 as week_start_544,
    dc.year_544,
    att.QBO_CHANNEL_SUPER_AGGR_NAME_aux AS QBO_CHANNEL_SUPER_AGGR_NAME,
    count(distinct case when login_excl_first_30d=1 and r.open_subscriber=1 then e.company_id end) AS logged_in_week

FROM SBG_PUBLISHED.qbo_raw_billing_events_daily as e

INNER JOIN SBG_DM.dim_calendar as dc
    on dc.date_for_day = e.event_date

INNER JOIN SBG_STABLE.qbo_company_status as cs
    on cs.qbo_company_id = e.company_id

INNER JOIN SBG_PUBLISHED.qbo_company_attributes_ftu as att
    on att.company_id = cs.QBO_COMPANY_ID

LEFT JOIN SBG_STABLE.qbo_reporting_daily as r
    on r.qbo_company_id = e.company_id
    and r.date_of = e.event_date

LEFT JOIN sbg_published.qbo_product_short_list qpsl
    on r.product_current = qpsl.product


WHERE e.event_date>=add_months(date_trunc('month',CURRENT_DATE),-12) and  att.country = 'Brazil'-- past 12 months and only Brazil

GROUP BY att.country,dc.week_start_date_544,dc.week_for_year_544,dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;


-- -- Monthly product metrics
INSERT /*+ DIRECT */ INTO qbobr_datainmetrics ( country, date_for_day, month_start,  year_fy,  QBO_CHANNEL_SUPER_AGGR_NAME, logged_in_month)
SELECT
    att.country,
    dc.month_start_date_fy AS date_for_day,  -- done intentionally
    dc.month_start_date_fy,
    dc.year_fy,
    att.QBO_CHANNEL_SUPER_AGGR_NAME_aux AS QBO_CHANNEL_SUPER_AGGR_NAME,
    count(distinct case when e.login_excl_first_30d=1 and r.open_subscriber=1 then e.company_id end) AS logged_in_month

FROM SBG_PUBLISHED.qbo_raw_billing_events_daily as e

INNER JOIN SBG_DM.dim_calendar as dc
    on dc.date_for_day = e.event_date

INNER JOIN SBG_STABLE.qbo_company_status as cs
    on cs.qbo_company_id = e.company_id

INNER JOIN SBG_PUBLISHED.qbo_company_attributes_ftu as att
    on att.company_id = cs.QBO_COMPANY_ID

LEFT JOIN SBG_STABLE.qbo_reporting_daily as r
    on r.qbo_company_id = e.company_id
    and r.date_of = e.event_date

LEFT JOIN sbg_published.qbo_product_short_list qpsl
    on r.product_current = qpsl.product

WHERE e.event_date>=add_months(date_trunc('month',CURRENT_DATE),-12) and  att.country = 'Brazil'-- past 12 months and only Brazil

GROUP BY att.country,dc.month_start_date_fy,dc.month_start_date_fy,dc.year_fy,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics ( country, date_for_day, week_num, week_start, month_start, month_start_calc, quarter_start, year_fy, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME,added_txn_past31d, added_invoice_past31d, added_expense_past31d, added_txn_past31d_mobileapp, added_invoice_past31d_mobileapp, added_expense_past31d_mobileapp, added_txn_past31d_web, added_invoice_past31d_web, added_expense_past31d_web)
with add_txn as
(
select
  rd.qbo_company_id,
  rd.date_of,
  1 as has_txn,
  qpsl.product_aux as product_current,
  -- split out main transaction types
  max(case when ai.tx_type_id = 4 then 1 else 0 end) as has_invoice,
  max(case when ai.tx_type_id = 54 then 1 else 0 end) as has_expense,
  -- mobile transactions
  max(case when ai.device_type_id in (1,2,3,7,9,13,14,15,16,17) then 1 else 0 end) as qbm_txn,
  max(case when ai.device_type_id in (1,2,3,7,9,13,14,15,16,17) and ai.tx_type_id = 4 then 1 else 0 end) as qbm_invoice,
  max(case when ai.device_type_id in (1,2,3,7,9,13,14,15,16,17) and ai.tx_type_id = 54 then 1 else 0 end) as qbm_expense,
  -- web transactions -- includes mamba (so basically everything but QBM)
  max(case when NVL(ai.device_type_id,0) not in (1,2,3,7,9,13,14,15,16,17) then 1 else 0 end) as web_txn,
  max(case when NVL(ai.device_type_id,0) not in (1,2,3,7,9,13,14,15,16,17) and ai.tx_type_id = 4 then 1 else 0 end) as web_invoice,
  max(case when NVL(ai.device_type_id,0) not in (1,2,3,7,9,13,14,15,16,17) and ai.tx_type_id = 54 then 1 else 0 end) as web_expense
from sbg_stable.qbo_reporting_daily rd
  join sbg_source.src_qbo_combined_auditinfo_vw ai on rd.qbo_company_id = ai.company_id and ai.audit_date between rd.date_of-30 and rd.date_of
  left join sbg_published.qbo_product_short_list qpsl on rd.product_current = qpsl.product
where rd.open_subscriber = 1 -- open sub on last day of the 31 day period
  and ai.action_type_id = 3 and ai.list_type_id = 7 -- adding a transaction
  and date(rd.date_of) between add_months(date_trunc('month',CURRENT_DATE),-23) and current_date - 1 -- past 23 months, vertica has truncated data (24 mo)
group by rd.qbo_company_id, rd.date_of,1,qpsl.product_aux
)
select
  att.country,
  dc.date_for_day,
  dc.week_for_year_fy as week_num,
  dc.week_start_date_fy as week_start,
  dc.month_start_date_fy as month_start,
  month(dc.month_start_date_fy) as month_start_calc,
  dc.quarter_start_date_fy as quarter_start,
  dc.year_fy,
  dc.week_for_year_544 as week_num_544,
  dc.week_start_date_544 as week_start_544,
  dc.year_544,
  att.qbo_channel_super_aggr_name_aux as qbo_channel_super_aggr_name,
  sum(t.has_txn) as added_txn_past31d,
  sum(t.has_invoice) as added_invoice_past31d,
  sum(t.has_expense) as added_expense_past31d,
  sum(t.qbm_txn) as added_txn_past31d_mobileapp,
  sum(t.qbm_invoice) as added_invoice_past31d_mobileapp,
  sum(t.qbm_expense) as added_expense_past31d_mobileapp,
  sum(t.web_txn) as added_txn_past31d_web,
  sum(t.web_invoice) as added_invoice_past31d_web,
  sum(t.web_expense) as added_expense_past31d_web

from add_txn t
  join sbg_stable.qbo_company_status cs on cs.qbo_company_id = t.qbo_company_id
  join sbg_published.qbo_company_attributes_ftu att on att.company_id = cs.qbo_company_id
  join sbg_dm.dim_calendar dc on t.date_of = dc.date_for_day
  join sbg_stable.qbo_reporting_daily r on t.qbo_company_id = r.qbo_company_id and r.date_of = t.date_of
  
  WHERE r.date_of>=add_months(date_trunc('month',CURRENT_DATE),-12) and  att.country = 'Brazil' -- past 12 months and only Brazil
  
GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics ( country, date_for_day, week_num, week_start, month_start, month_start_calc, quarter_start, year_fy, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME, manual_txns, total_txns)
select
  att.country,
  dc.date_for_day,
  dc.week_for_year_fy as week_num,
  dc.week_start_date_fy as week_start,
  dc.month_start_date_fy as month_start,
  month(dc.month_start_date_fy) as month_start_calc,
  dc.quarter_start_date_fy as quarter_start,
  dc.year_fy,
  dc.week_for_year_544 as week_num_544,
  dc.week_start_date_544 as week_start_544,
  dc.year_544,
  att.qbo_channel_super_aggr_name_aux as qbo_channel_super_aggr_name,
  sum(txn.manual_txns) as manual_txns,
  sum(txn.ttl_txns) as total_txns
from sbg_published.qbo_company_txn_source_by_day txn
  join sbg_stable.qbo_company_status cs on cs.qbo_company_id = txn.company_id
  join sbg_published.qbo_company_attributes_ftu att on att.company_id = cs.qbo_company_id
  join sbg_dm.dim_calendar dc on txn.date_for_day = dc.date_for_day
  join sbg_stable.qbo_reporting_daily r on cs.qbo_company_id = r.qbo_company_id and r.date_of = dc.date_for_day
  left join sbg_published.qbo_product_short_list qpsl on r.product_current = qpsl.product
 
where txn.if_addressable = 1 and date(txn.date_for_day) between add_months(date_trunc('month',CURRENT_DATE),-12) and current_date - 16 and att.country = 'Brazil' -- past 36 months, metric has 15 day baking period
GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics ( country, date_for_day, week_num, week_start, month_start, month_start_calc, quarter_start, year_fy, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME, bank_txns_added, bank_txns_added_auto_rule)

with temp as
(
select company_id, date(create_date) as date_for_day,
  count(distinct tx_id) as bank_txns_added,
  count(distinct case when olb_match_mode = 5 then tx_id end) as bank_txns_added_auto_rule
from sbg_source.src_qbo_combined_txdetails_vw txn
where olb_match_mode in (1,2,4,3,5,6)
  and date(create_date) between add_months(date_trunc('month',CURRENT_DATE),-36) and current_date - 1 -- past 36 months
group by company_id,date(create_date) order by company_id,date(create_date)
)
select
  att.country,
  dc.date_for_day,
  dc.week_for_year_fy as week_num,
  dc.week_start_date_fy as week_start,
  dc.month_start_date_fy as month_start,
  month(dc.month_start_date_fy) as month_start_calc,
  dc.quarter_start_date_fy as quarter_start,
  dc.year_fy,
  dc.week_for_year_544 as week_num_544,
  dc.week_start_date_544 as week_start_544,
  dc.year_544,
  att.qbo_channel_super_aggr_name_aux as qbo_channel_super_aggr_name,
  sum(bank_txns_added) as bank_txns_added,
  sum(bank_txns_added_auto_rule) as bank_txns_added_auto_rule
from temp txn
  join sbg_stable.qbo_company_status cs on cs.qbo_company_id = txn.company_id
  join sbg_published.qbo_company_attributes_ftu att on att.company_id = cs.qbo_company_id
  join sbg_dm.dim_calendar dc on txn.date_for_day = dc.date_for_day
  left join sbg_stable.qbo_reporting_daily r on r.qbo_company_id = cs.qbo_company_id and r.date_of = dc.date_for_day
  left join sbg_published.qbo_product_short_list qpsl on r.product_current = qpsl.product
WHERE r.date_of>=add_months(date_trunc('month',CURRENT_DATE),-12) and  att.country = 'Brazil' -- past 12 months and only Brazil
GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux
