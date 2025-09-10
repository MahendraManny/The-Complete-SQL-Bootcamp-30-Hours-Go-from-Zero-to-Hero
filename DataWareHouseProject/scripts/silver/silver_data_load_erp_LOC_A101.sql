insert into datawarehouse.silver.erp_LOC_A101
(
CID,
CNTRY
)
select
a.*
from(
select replace(CID,'-','') as CID,
case
	when CNTRY = 'DE' then 'Germany'
	when CNTRY = 'USA' or CNTRY = 'US' then 'United States'
	when CNTRY is null or CNTRY = '' then 'n/a'
	else CNTRY
end as CNTRY
from datawarehouse.bronze.erp_LOC_A101) a ;