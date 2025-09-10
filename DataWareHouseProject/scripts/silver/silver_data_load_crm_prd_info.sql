insert into datawarehouse.silver.crm_prd_info(
prd_id,
cat_id,
prd_key, 
prd_nm,
prd_cost, 
prd_line, 
prd_start_dt, 
prd_end_dt 
)
select
prd_id,
replace(left(prd_key,5),'-','_') as cat_id, -- extract category ID
SUBSTRING(prd_key,7,len(prd_key)) as prd_key, -- extract product key	
prd_nm,
isnull(prd_cost,0),
case
	when upper(trim(prd_line)) = 'M' then 'Mountain'
	when upper(trim(prd_line)) = 'R' then 'Road'
	when upper(trim(prd_line)) = 'S' then 'Other Sales'
	when upper(trim(prd_line)) = 'T' then  'Touring'
	else 'n/a'
end as prd_line,
prd_start_dt,
dateadd(day,-1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt
from datawarehouse.bronze.crm_prd_info;