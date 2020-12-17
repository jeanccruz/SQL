WITH qbo_br as (
  select
    qbo.qbo_company_id
  from SBG_STABLE.QBO_COMPANY_STATUS qbo
  where qbo.qbo_country = 'Brazil'
),

br_company_invoices as (
  select 
    year(tx.create_date),
    month(tx.create_date),
    qbo_company_id,
    count( 
      CASE WHEN (tx.is_ar_paid = 1) 
      THEN 1 
      ELSE NULL END) as invoices_paid
      
  from SBG_SOURCE.src_qbo_combined_txheaders_vw tx
  join qbo_br on qbo_br.qbo_company_id = tx.company_id
  where 
    tx.create_date >= '2020-01-01' and
    tx.create_date <= LAST_DAY(ADD_MONTHS(current_date, -1)) and
    tx_type_id = 4
    
  group by 1, 2, 3
  order by 1, 2, 3
)
select 
  bci.year,
  bci.month,
  att.QBO_CHANNEL_SUPER_AGGR_NAME_aux AS QBO_CHANNEL_SUPER_AGGR_NAME,
  att.signup_type_aux as qbo_signup_type_desc,
  sta.qbo_company_current_status,
  count(bci.qbo_company_id) as invoicing_companies,
  count(CASE WHEN (bci.invoices_paid >= 1)
        THEN 1
        ELSE NULL END) as invoicing_companies_got_paid

from br_company_invoices bci
INNER JOIN SBG_PUBLISHED.qbo_company_attributes_ftu as att
    on att.company_id = bci.qbo_company_id
INNER JOIN SBG_STABLE.QBO_COMPANY_STATUS as sta
    on sta.QBO_COMPANY_ID = bci.qbo_company_id
group by 1, 2, 3, 4, 5
order by 1, 2, 3, 4, 5

