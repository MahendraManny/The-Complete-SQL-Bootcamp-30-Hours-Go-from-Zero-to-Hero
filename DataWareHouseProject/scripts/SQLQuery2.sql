alter table datawarehouse.bronze.erp_CUST_AZ12 alter column BDATE DATE;


select *
from 

(

select
case
	when CID like 'NAS%' then substring(CID,4,len(CID))
	else CID
end as CID,
case 
	when BDATE > GETDATE() then 'n/a'
	else BDATE
end as BDATE
from datawarehouse.bronze.erp_CUST_AZ12 ) a

where BDATE > getdate();