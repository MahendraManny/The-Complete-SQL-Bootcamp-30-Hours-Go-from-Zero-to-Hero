insert into datawarehouse.silver.erp_CUST_AZ12
(
CID,
BDATE,
GEN
)
select 
case
	when CID like 'NAS%' then SUBSTRING(CID,4,len(CID))
	else CID
end as CID,
case
	when BDATE > getdate() then NULL
	else BDATE
end as BDATE,
case
	when GEN is null or GEN = '' then 'n/a'
	when GEN = 'F' then 'Female'
	when GEN = 'M' then 'Male'
	else GEN
end as GEN	
from datawarehouse.bronze.erp_CUST_AZ12