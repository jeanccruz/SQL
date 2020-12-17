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
     count(*) as total_invoices,
     count( 
      CASE WHEN (tx.is_ar_paid = 1) 
      THEN 1 
      ELSE NULL END) as invoices_paid,
     count(
      CASE WHEN (b.id is not null)
      THEN 1
      ELSE NULL END) as num_invoices_with_boleto
  from SBG_SOURCE.src_qbo_combined_txheaders_vw tx
  join qbo_br on qbo_br.qbo_company_id = tx.company_id
  left join SBG_PUBLISHED.br_boleto_settings bs on bs.company_id = tx.company_id
  left join SBG_PUBLISHED.br_boleto_boleto b on (tx.tx_id = b.transaction_id and bs.id = b.settings_id)
  left join SBG_PUBLISHED.br_notafiscalv2_settings ns on ns.company_id = tx.company_id
  left join SBG_PUBLISHED.br_notafiscalv2_nota_fiscal nf on (tx.tx_id = nf.transaction_id and ns.id = nf.settings_id)
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
    SUM(BCI.TOTAL_INVOICES) AS TOTAL_INVOICES,
  sum(bci.num_invoices_with_boleto) AS INVOICES_WITH_boleto,
  sum(CASE WHEN (bci.num_invoices_with_boleto >= 1) THEN bci.total_invoices else null end) as TOTAL_INVOICES_FOR_BOLETO_ISSUERS,
  count(CASE WHEN (bci.num_invoices_with_boleto >= 1)
        THEN 1
        ELSE NULL END) as boleto_issuers,
  count(CASE WHEN (bci.num_invoices_with_boleto >= 1 and bci.invoices_paid >= 1)
        THEN 1
        ELSE NULL END) as boleto_issuers_got_paid,
  sum(bci.num_invoices_with_boleto) / sum(CASE WHEN (bci.num_invoices_with_boleto >= 1) THEN bci.total_invoices else null end) as percent_invoices_with_boleto_for_issuers
from br_company_invoices bci
INNER JOIN SBG_PUBLISHED.qbo_company_attributes_ftu as att
    on att.company_id = bci.qbo_company_id

    group by 1, 2, 3
order by 1, 2, 3
