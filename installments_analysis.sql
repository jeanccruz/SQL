--------------------------------------------------------------------

with filter as (
select b.country,b.qbo_channel_super_aggr_name_aux, a.company_id, c.tx_type_id,a.company_id || '-' || a.charge_tx_id as company_txid_key, a.create_date 
from SBG_PUBLISHED.qbogbl_installmentschedules a
join SBG_PUBLISHED.qbo_company_attributes_ftu b on b.company_id = a.company_id
join SBG_SOURCE.src_qbo_combined_txheaders_vw c on c.company_id = a.company_id and c.tx_id = a.charge_tx_id
where b.country = 'Brazil'
order by 1,2,3,4,5
              ),

usage_log as (
select company_id,
       qbo_channel_super_aggr_name_aux,
       tx_type_id,
       datediff(month,'2020-07-01',create_date) as usage_month
       from filter
       group by 1,2,3,4
       order by 1,2,3,4
       ),
first_usage as (
select company_id,
       qbo_channel_super_aggr_name_aux,
       tx_type_id,
       min(usage_month) as first_month
       from usage_log
       group by 1,2,3
       order by 1,2,3
       ),
new_users as (
select first_month,
       qbo_channel_super_aggr_name_aux,
       tx_type_id,
       count(distinct company_id) as new_users
       from first_usage
       group by 1,2,3
       order by 1,2,3
       )
select first_usage.first_month,
       usage_log.qbo_channel_super_aggr_name_aux,
       usage_log.tx_type_id,
       new_users.new_users,
       usage_log.usage_month - first_usage.first_month as retention_month,
       count(distinct usage_log.company_id) as retained,
       count(distinct usage_log.company_id)/new_users.new_users as retention_percent
       from first_usage
       left join usage_log on usage_log.company_id = first_usage.company_id and usage_log.qbo_channel_super_aggr_name_aux = first_usage.qbo_channel_super_aggr_name_aux and usage_log.tx_type_id = first_usage.tx_type_id
       left join new_users on new_users.first_month = first_usage.first_month and new_users.qbo_channel_super_aggr_name_aux = first_usage.qbo_channel_super_aggr_name_aux and new_users.tx_type_id = first_usage.tx_type_id
       group by 1,2,3,4,5
       order by 1,2,3,4,5
