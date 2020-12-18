SET search_path to SBG_PUBLISHED;

DROP TABLE IF EXISTS qbobr_datainmetrics_all_countries;
CREATE TABLE  qbobr_datainmetrics_all_countries AS /*+ DIRECT */
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

WHERE r.date_of>=add_months(date_trunc('month',CURRENT_DATE),-12) and att.country in ( 'Brazil' , 'Mexico' , 'Canada' ) -- past 12 months and only Brazil

GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT ;

ALTER TABLE qbobr_datainmetrics_all_countries add COLUMN added_txn_past31d INT;  
ALTER TABLE qbobr_datainmetrics_all_countries add COLUMN added_txn_past31d_web INT;
ALTER TABLE qbobr_datainmetrics_all_countries add COLUMN total_txns INT;
ALTER TABLE qbobr_datainmetrics_all_countries add COLUMN manual_txns INT;
ALTER TABLE qbobr_datainmetrics_all_countries add COLUMN bank_txns_added INT;
ALTER TABLE qbobr_datainmetrics_all_countries add COLUMN bank_txns_added_auto_rule INT;


COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics_all_countries ( country, date_for_day, month_start, month_start_calc, year_fy, QBO_CHANNEL_SUPER_AGGR_NAME,added_txn_past31d, added_txn_past31d_web)
with add_txn as
(
select
  rd.qbo_company_id,
  rd.date_of,
  1 as has_txn,
  max(case when NVL(ai.device_type_id,0) not in (1,2,3,7,9,13,14,15,16,17) then 1 else 0 end) as web_txn
from sbg_stable.qbo_reporting_daily rd
  join sbg_source.src_qbo_combined_auditinfo_vw ai on rd.qbo_company_id = ai.company_id and ai.audit_date between rd.date_of-30 and rd.date_of
where rd.open_subscriber = 1 -- open sub on last day of the 31 day period
  and ai.action_type_id = 3 and ai.list_type_id = 7 -- adding a transaction
  and date(rd.date_of) between add_months(date_trunc('month',CURRENT_DATE),-12) and current_date - 1 -- past 12 months
group by rd.qbo_company_id, rd.date_of,1
)
select
  att.country,
  dc.date_for_day,
  dc.month_start_date_fy as month_start,
  month(dc.month_start_date_fy) as month_start_calc,
  dc.year_fy,
  att.qbo_channel_super_aggr_name_aux as qbo_channel_super_aggr_name,
  sum(t.has_txn) as added_txn_past31d,
  sum(t.web_txn) as added_txn_past31d_web
from add_txn t
  join sbg_stable.qbo_company_status cs on cs.qbo_company_id = t.qbo_company_id
  join sbg_published.qbo_company_attributes_ftu att on att.company_id = cs.qbo_company_id
  join sbg_dm.dim_calendar dc on t.date_of = dc.date_for_day
  join sbg_stable.qbo_reporting_daily r on t.qbo_company_id = r.qbo_company_id and r.date_of = t.date_of
  
  WHERE r.date_of>=add_months(date_trunc('month',CURRENT_DATE),-12) and att.country in ( 'Brazil' , 'Mexico' , 'Canada' ) -- past 12 months 
  
GROUP BY att.country,dc.date_for_day,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.year_fy,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics_all_countries ( country, date_for_day, week_num, week_start, month_start, month_start_calc, quarter_start, year_fy, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME, manual_txns, total_txns)
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
 
where txn.if_addressable = 1 and date(txn.date_for_day) between add_months(date_trunc('month',CURRENT_DATE),-12) and current_date - 16 and att.country in ( 'Brazil' , 'Mexico' , 'Canada' )  -- past 12 months
GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

COMMIT;

INSERT /*+ DIRECT */ INTO qbobr_datainmetrics_all_countries ( country, date_for_day, week_num, week_start, month_start, month_start_calc, quarter_start, year_fy, week_num_544, week_start_544, year_544, QBO_CHANNEL_SUPER_AGGR_NAME, bank_txns_added, bank_txns_added_auto_rule)
with temp as
(
select company_id, date(create_date) as date_for_day,
  count(distinct tx_id) as bank_txns_added,
  count(distinct case when olb_match_mode = 5 then tx_id end) as bank_txns_added_auto_rule
from sbg_source.src_qbo_combined_txdetails_vw txn
where olb_match_mode in (1,2,4,3,5,6)
  and date(create_date) between add_months(date_trunc('month',CURRENT_DATE),-12) and current_date - 1 -- past 36 months
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
WHERE r.date_of>=add_months(date_trunc('month',CURRENT_DATE),-12) and att.country in ( 'Brazil' , 'Mexico' , 'Canada' ) -- past 12 months and only Brazil
GROUP BY att.country,dc.date_for_day,dc.week_for_year_fy,dc.week_start_date_fy,dc.month_start_date_fy,month(dc.month_start_date_fy),dc.quarter_start_date_fy,dc.year_fy,dc.week_for_year_544, dc.week_start_date_544,dc.year_544,att.QBO_CHANNEL_SUPER_AGGR_NAME_aux;

