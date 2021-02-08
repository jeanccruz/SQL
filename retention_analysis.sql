-- companies logins while sub

SELECT
  login.company_id,
  login.login,
  login.event_date,
  status.qbo_gns_date,
  status.qbo_channel_aggr_name,
  MAX(CASE
                   WHEN login.event_date BETWEEN status.qbo_gns_date and status.qbo_gns_date +7
                       THEN 1
                   ELSE 0 END) AS logins_first_7days
  FROM SBG_PUBLISHED.qbo_raw_billing_events_daily login
  LEFT JOIN SBG_STABLE.qbo_company_status status on login.company_id = status.qbo_company_id
  WHERE status.qbo_country = 'Brazil' and year(status.qbo_gns_date) = 2020 and month(status.qbo_gns_date) in ( 1,2,3,4,5,6,7,8,9 )
  group by 1,2,3,4,5
  
  
  

--querying companies actions
with stg_completed_actions as (SELECT
    a.qbo_company_id                       AS qbo_company_id,
    SUM(a.email_invoice_cnt)               AS inv_count,
    SUM(a.add_expense_cnt)                 AS add_expense_cnt,
    SUM(a.add_deposit_cnt)                 AS add_deposit_cnt,
    SUM(a.add_check_cnt)                   AS add_check_cnt,
    SUM(a.add_recv_payment_cnt)            AS add_recv_payment_cnt,
    SUM(a.add_journal_cnt)                 AS add_journal_cnt,
    SUM(a.add_bill_cnt)                    AS add_bill_cnt,
    SUM(a.add_transfer_cnt)                AS add_transfer_cnt,
    SUM(a.add_tax_payment_cnt)             AS add_tax_payment_cnt,
    SUM(a.add_bill_payment_cnt)            AS add_bill_payment_cnt,
    SUM(a.add_creditcard_credit_cnt)       AS add_creditcard_credit_cnt,
    SUM(a.add_memorized_transaction_cnt)   AS add_memorized_transaction_cnt,
    SUM(a.add_payroll_check_cnt)           AS add_payroll_check_cnt,
    SUM(a.add_estimate_cnt)                AS add_estimate_cnt,
    SUM(a.add_sales_receipt_cnt)           AS add_sales_receipt_cnt,
    SUM(a.add_credit_memo_cnt)             AS add_credit_memo_cnt,
    SUM(a.add_purchase_order_cnt)          AS add_purchase_order_cnt,
    SUM(a.add_olbr_cnt)                    AS add_olbr_cnt,
    SUM(a.add_account_cnt)                 AS add_account_cnt,
    SUM(a.add_item_cnt)                    AS add_item_cnt,
    SUM(a.add_payment_method_cnt)          AS add_payment_method_cnt,
    SUM(a.add_customer_cnt)                AS add_customer_cnt,
    SUM(a.add_vendor_cnt)                  AS add_vendor_cnt,
    SUM(a.add_employee_cnt)                AS add_employee_cnt,
    SUM(a.add_user_cnt)                    AS add_user_cnt,
    SUM(a.add_non_accountant_user_cntthen) AS add_non_accountant_user_cntthen,
    SUM(a.add_accountant_user_cnt)         AS add_accountant_user_cnt,
    SUM(a.add_attachment_cnt)              AS add_attachment_cnt,
    SUM(a.add_tax_agency_cnt)              AS add_tax_agency_cnt,
    SUM(a.add_tax_rate_cnt)                AS add_tax_rate_cnt,
    SUM(a.add_tax_code_cnt)                AS add_tax_code_cnt,
    SUM(a.add_term_cnt)                    AS add_term_cnt,
    SUM(a.add_klass_cnt)                   AS add_klass_cnt,
    SUM(a.add_memorized_report_cnt)        AS add_memorized_report_cnt,
    SUM(a.add_time_activity_cnt)           AS add_time_activity_cnt,
    SUM(a.add_statement_cnt)               AS add_statement_cnt,
    SUM(a.add_budget_cnt)                  AS add_budget_cnt,
    SUM(a.add_invoice_cnt)                 AS add_invoice_cnt,
    SUM(a.edit_invoice_cnt)                AS edit_invoice_cnt
FROM
    sbg_dm.fact_qbo_company_product_usage_daily a
    
LEFT JOIN SBG_STABLE.qbo_company_status B on a.qbo_company_id = b.qbo_company_id
WHERE b.qbo_country = 'Brazil' and year(b.qbo_gns_date) = 2020 and month(b.qbo_gns_date) in ( 1,2,3,4,5,6 , 7,8,9 ) and a.usage_date_key BETWEEN b.qbo_gns_date and b.qbo_gns_date + 93
GROUP BY
    a.qbo_company_id)

SELECT
    sub.qbo_company_id,
    sub.action_name_simple,
    sub.action_name,
    sub.cnt,
    status.qbo_gns_date,
    status.qbo_channel_aggr_name
FROM
    (
        SELECT
            qbo_company_id,
            cnt,
            action_name as action_name_simple,
            concat(concat(action_name,TO_CHAR(' - ')),TO_CHAR(cnt))      AS action_name,
            RANK() OVER( PARTITION BY qbo_company_id ORDER BY cnt DESC )    rank_action --Ranking
            -- by number of times action is perform
        FROM
            (
                SELECT
                    qbo_company_id,
                    add_expense_cnt   AS cnt,
                    'add_expense_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_expense_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_deposit_cnt   AS cnt,
                    'add_deposit_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_deposit_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_check_cnt   AS cnt,
                    'add_check_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_check_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_recv_payment_cnt   AS cnt,
                    'add_recv_payment_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_recv_payment_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_journal_cnt   AS cnt,
                    'add_journal_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_journal_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_bill_cnt   AS cnt,
                    'add_bill_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_bill_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_transfer_cnt   AS cnt,
                    'add_transfer_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_transfer_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_tax_payment_cnt   AS cnt,
                    'add_tax_payment_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_tax_payment_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_bill_payment_cnt   AS cnt,
                    'add_bill_payment_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_bill_payment_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_creditcard_credit_cnt   AS cnt,
                    'add_creditcard_credit_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_creditcard_credit_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_memorized_transaction_cnt   AS cnt,
                    'add_memorized_transaction_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_memorized_transaction_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_payroll_check_cnt   AS cnt,
                    'add_payroll_check_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_payroll_check_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_estimate_cnt   AS cnt,
                    'add_estimate_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_estimate_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_sales_receipt_cnt   AS cnt,
                    'add_sales_receipt_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_sales_receipt_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_credit_memo_cnt   AS cnt,
                    'add_credit_memo_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_credit_memo_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_purchase_order_cnt   AS cnt,
                    'add_purchase_order_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_purchase_order_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_olbr_cnt   AS cnt,
                    'add_olbr_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_olbr_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_account_cnt   AS cnt,
                    'add_account_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_account_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_item_cnt   AS cnt,
                    'add_item_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_item_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_payment_method_cnt   AS cnt,
                    'add_payment_method_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_payment_method_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_customer_cnt   AS cnt,
                    'add_customer_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_customer_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_vendor_cnt   AS cnt,
                    'add_vendor_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_vendor_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_employee_cnt   AS cnt,
                    'add_employee_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_employee_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_user_cnt   AS cnt,
                    'add_user_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_user_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_non_accountant_user_cntthen   AS cnt,
                    'add_non_accountant_user_cntthen' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_non_accountant_user_cntthen >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_accountant_user_cnt   AS cnt,
                    'add_accountant_user_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_accountant_user_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_attachment_cnt   AS cnt,
                    'add_attachment_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_attachment_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_tax_agency_cnt   AS cnt,
                    'add_tax_agency_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_tax_agency_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_tax_rate_cnt   AS cnt,
                    'add_tax_rate_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_tax_rate_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_tax_code_cnt   AS cnt,
                    'add_tax_code_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_tax_code_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_term_cnt   AS cnt,
                    'add_term_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_term_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_klass_cnt   AS cnt,
                    'add_klass_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_klass_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_memorized_report_cnt   AS cnt,
                    'add_memorized_report_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_memorized_report_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_time_activity_cnt   AS cnt,
                    'add_time_activity_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_time_activity_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_statement_cnt   AS cnt,
                    'add_statement_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_statement_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_budget_cnt   AS cnt,
                    'add_budget_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_budget_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    add_invoice_cnt   AS cnt,
                    'add_invoice_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    add_invoice_cnt >0
                UNION ALL
                SELECT
                    qbo_company_id,
                    edit_invoice_cnt   AS cnt,
                    'edit_invoice_cnt' AS action_name
                FROM
                    stg_completed_actions
                WHERE
                    edit_invoice_cnt >0 ) a ) sub
LEFT JOIN SBG_STABLE.qbo_company_status status on sub.qbo_company_id = status.qbo_company_id
WHERE status.qbo_country = 'Brazil' and year(status.qbo_gns_date) = 2020 and month(status.qbo_gns_date) in ( 1,2,3,4,5,6 , 7,8,9 )
    --sub.rank_action < 10 -- limiting to top 5 actions
AND sub.cnt > 0 -- removing where action is not even performed once
order by sub.qbo_company_id, sub.cnt;
            
 
 
 
 

-- testing if the results match with E2E Funnel Report;
SELECT count(distinct qbo_company_id) FROM SBG_STABLE.qbo_company_status
WHERE qbo_country = 'Brazil' and year(qbo_gns_date) = 2020 and month(qbo_gns_date)=6;

-- checking the offers table to find the months (4,5,6) offers
select * from SBG_DM.dim_offer
where offer_id = 20020064

select * from SBG_dm.qbo_company_product_usage_vw limit 10;
--where year(offer_start_datetime) = 2020 and month(offer_start_datetime) = 4 and offer_region_code = 'BR';

-- selecting company_id with GNS date in 2020 months (4,5,6) and their offer info;
with original_offer as (
select * from sbg_dm.dim_offer
where offer_region_code = 'BR'),

current_offer as (
select * from sbg_dm.dim_offer
where offer_region_code = 'BR')

select
a.qbo_company_id,
a.qbo_country,
a.qbo_signup_date,
a.qbo_gns_date,
a.qbo_cancel_date,
a.qbo_subscription_type_desc,
a.qbo_signup_type_desc,
a.qbo_channel_name,
a.qbo_channel_aggr_name,
a.qbo_current_product,
b.qbo_channel_super_aggr_name_aux,
a.qbo_channel_super_aggr_name,
a.qbo_migrator_type_description,
a.original_offer_id,
a.current_offer_id,
a.qbo_active_cancel_date,
a.qbo_company_current_status,
c.offer_name as O_offer_name,
c.offer_type as O_offer_type,
c.offer_start_datetime as O_offer_start_datetime,
c.offer_end_datetime as O_offer_end_datetime,
c.customer_segment as O_customer_segment,
c.offer_base_price_amt as O_offer_base_price_amt,
c.offer_charge_type as O_offer_charge_type,
c.offer_charge_frequency as O_offer_charge_frequency,
c.discount_desc as O_discount_desc,
c.discount_amt as O_discount_amt,
c.discount_duration_nbr as O_discount_duration_nbr,
c.discount_duration_units as O_discount_duration_units,
d.offer_name as C_offer_name,
d.offer_type as C_offer_type,
d.offer_start_datetime as C_offer_start_datetime,
d.offer_end_datetime as C_offer_end_datetime,
d.customer_segment as C_customer_segment,
d.offer_base_price_amt as C_offer_base_price_amt,
d.offer_charge_type as C_offer_charge_type,
d.offer_charge_frequency as C_offer_charge_frequency,
d.discount_desc as C_discount_desc,
d.discount_amt as C_discount_amt,
d.discount_duration_nbr as C_discount_duration_nbr,
d.discount_duration_units as C_discount_duration_units,
MAX(CASE
                   WHEN a.qbo_gns_date + 93 BETWEEN a.qbo_gns_date and coalesce(a.qbo_cancel_date, '2050-01-01')
                       THEN 1
                   ELSE 0 END) AS gns_retained_day92
from SBG_STABLE.qbo_company_status a
left join SBG_PUBLISHED.qbo_company_attributes_ftu b on a.qbo_company_id = b.company_id
left join original_offer c on a.original_offer_id = c.offer_id
left join current_offer d on a.current_offer_id = d.offer_id
WHERE a.qbo_country = 'Brazil' and year(a.qbo_gns_date) = 2020 --and month(a.qbo_gns_date) in ()
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41;


--- company usage per cohorted group
SELECT
    a.qbo_company_id,
    b.qbo_channel_aggr_name,
    B.qbo_gns_date,
    b.qbo_cancel_date,
    --a.usage_date_key,
    --datediff(day,B.qbo_gns_date, A.usage_date_key) AS DAYS,
    datediff(day,b.qbo_gns_date,coalesce(b.qbo_cancel_date, CURRENT_DATE)) as GNS_days,
    SUM(a.email_invoice_cnt)+ 
    SUM(a.add_expense_cnt)+                 
    SUM(a.add_deposit_cnt)+               
    SUM(a.add_check_cnt)+                   
    SUM(a.add_recv_payment_cnt)+            
    SUM(a.add_journal_cnt)+                 
    SUM(a.add_bill_cnt)+                    
    SUM(a.add_transfer_cnt)+                
    SUM(a.add_tax_payment_cnt)+             
    SUM(a.add_bill_payment_cnt)+            
    SUM(a.add_creditcard_credit_cnt)+       
    SUM(a.add_memorized_transaction_cnt)+  
    SUM(a.add_payroll_check_cnt)+           
    SUM(a.add_estimate_cnt)+                
    SUM(a.add_sales_receipt_cnt)+           
    SUM(a.add_credit_memo_cnt)+             
    SUM(a.add_purchase_order_cnt)+         
    SUM(a.add_olbr_cnt)+                    
    SUM(a.add_account_cnt)+                
    SUM(a.add_item_cnt)+                  
    SUM(a.add_payment_method_cnt)+          
    SUM(a.add_customer_cnt)+                
    SUM(a.add_vendor_cnt)+                  
    SUM(a.add_employee_cnt)+                
    SUM(a.add_user_cnt)+                  
    SUM(a.add_non_accountant_user_cntthen)+ 
    SUM(a.add_accountant_user_cnt)+         
    SUM(a.add_attachment_cnt)+              
    SUM(a.add_tax_agency_cnt)+             
    SUM(a.add_tax_rate_cnt)+                
    SUM(a.add_tax_code_cnt)+                
    SUM(a.add_term_cnt)+                    
    SUM(a.add_klass_cnt)+                   
    SUM(a.add_memorized_report_cnt)+        
    SUM(a.add_time_activity_cnt)+           
    SUM(a.add_statement_cnt)+               
    SUM(a.add_budget_cnt)+                  
    SUM(a.add_invoice_cnt)+                 
    SUM(a.edit_invoice_cnt) as main_actions             
FROM
    sbg_dm.fact_qbo_company_product_usage_daily a
    
LEFT JOIN SBG_STABLE.qbo_company_status B on a.qbo_company_id = b.qbo_company_id
WHERE b.qbo_country = 'Brazil' and year(b.qbo_gns_date) = 2020 and month(b.qbo_gns_date) in ( 1,2,3,4,5,6 , 7,8,9 ) and a.usage_date_key BETWEEN b.qbo_gns_date and b.qbo_gns_date + 93
GROUP BY
    a.qbo_company_id,b.qbo_channel_aggr_name,B.QBO_GNS_DATE,b.qbo_cancel_date--, a.usage_date_key
ORDER BY
    a.qbo_company_id,b.qbo_channel_aggr_name,B.QBO_GNS_DATE,b.qbo_cancel_date--, a.usage_date_key
