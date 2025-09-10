select *
from datawarehouse.bronze.crm_prd_info;

--checking duplicates in prd_id

select 
prd_id,
count(prd_id) as count_id
from datawarehouse.bronze.crm_prd_info
group by prd_id
having count(*) > 1;

---- checking unwanted spaces

select 
*
from datawarehouse.bronze.crm_prd_info
where prd_key != trim(prd_key);


select 
*
from datawarehouse.bronze.crm_prd_info
where prd_nm != trim(prd_nm);


select
prd_end_dt,
dateadd(day,-1,prd_end_dt) as previouse_data 
from datawarehouse.bronze.crm_prd_info;


alter table datawarehouse.bronze.crm_prd_info alter column prd_start_dt date;
alter table datawarehouse.bronze.crm_prd_info alter column prd_end_dt date;


select *
from datawarehouse.bronze.crm_prd_info;

select *
from datawarehouse.silver.crm_cust_info;



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


select *
from datawarehouse.bronze.crm_sales_details;


select *
from datawarehouse.bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num);

select *
from datawarehouse.bronze.crm_sales_details
where sls_prd_key != trim(sls_prd_key);


select *
from datawarehouse.bronze.crm_sales_details;

select *
from datawarehouse.silver.crm_sales_details;

insert into datawarehouse.silver.crm_sales_details
(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
concat(
left(cast(sls_order_dt as varchar),4),'-', ---year
substring(cast(sls_order_dt as varchar),5,2),'-', --month
substring(cast(sls_order_dt as varchar),7,2) --date
) as sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from datawarehouse.bronze.crm_sales_details;


select *
from datawarehouse.bronze.crm_sales_details
where len(sls_order_dt) != 8;



select
case
	when sls_order_dt is null or len(sls_order_dt) != 8 then null
	else concat(
				left(cast(sls_order_dt as varchar),4),'-', ---year
				substring(cast(sls_order_dt as varchar),5,2),'-', --month
				substring(cast(sls_order_dt as varchar),7,2) --date
				)
end as sls_order_dt
from datawarehouse.bronze.crm_sales_details;


select
*
from datawarehouse.bronze.crm_sales_details;


with sales_quantity_price_clean_up as(
select 
case
	when sls_sales <= 0 then 0	
	else sls_sales
end as sls_sales,
case
	when sls_quantity <= 0 then 0	
	else sls_quantity
end as sls_quantity,
case
	when sls_price <= 0 then 0	
	else sls_price
end as sls_price
from datawarehouse.bronze.crm_sales_details)


select *
from datawarehouse.bronze.crm_sales_details;




select
case
	when sls_sales is null 
		or sls_sales <= 0
		or abs(sls_sales) != abs(sls_quantity * sls_price)
	then abs(sls_quantity * sls_price)
	else sls_sales
end as sls_sales
from datawarehouse.bronze.crm_sales_details;


select
case
	when sls_quantity is null
		or sls_quantity <=0
		or abs(sls_quantity) != abs(sls_sales / nullif(sls_price,0))
		then abs(sls_sales / nullif(sls_price,0))
	else sls_quantity
end as sls_quantity
from datawarehouse.bronze.crm_sales_details;

select 
case	
	when sls_price is null
		or sls_price <=0
		or abs(sls_price) != abs(sls_sales/nullif(sls_quantity,0))
		then abs(sls_sales/nullif(sls_quantity,0))
	else sls_price
end as sls_price
from datawarehouse.bronze.crm_sales_details;


select
*
from datawarehouse.bronze.crm_sales_details;


with cleaning_up_cte as (
select
case
	when sls_sales = 0 then NULL
	when sls_sales < 0 then abs(sls_sales)
	else sls_sales
end as sls_sales,

case
	when sls_quantity = 0 then NULL
	when sls_quantity < 0 then abs(sls_quantity)
	else sls_quantity
end as sls_quantity,

case
	when sls_price = 0 then NULL
	when sls_price < 0 then abs(sls_price)
	else sls_price
end as sls_price

from datawarehouse.bronze.crm_sales_details
),

cleaned_up_cte_with_quality_checks as (

select
case
	when sls_sales != sls_quantity * sls_price then sls_quantity * sls_price
	else sls_sales
end as sls_sales,
case
	when sls_quantity != sls_sales / sls_price then sls_sales / sls_price
	else sls_quantity
end as sls_quantity,
case
	when sls_price != sls_sales / sls_quantity then sls_sales / sls_quantity
	else sls_price
end as sls_price
from cleaning_up_cte
),

final_cleaned_up_cte as (
select
case
	when sls_sales is null then sls_quantity * sls_price
	else sls_sales
end as sls_sales,
case
	when sls_quantity is null then sls_sales / sls_price
	else sls_quantity
end as sls_quantity,
case
	when sls_price is null then sls_sales / sls_quantity
	else sls_price
end as sls_price
from cleaned_up_cte_with_quality_checks

)

select 
*
from final_cleaned_up_cte
where sls_sales is null or sls_quantity is null or sls_price is null;




select *
from datawarehouse.bronze.crm_sales_details;


select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
case
	when sls_order_dt is null or len(sls_order_dt) > 8 then NULL
	else cast(concat(left(cast(sls_order_dt as varchar),4), '-',substring(cast(sls_order_dt as varchar),5,2),'-',substring(cast(sls_order_dt as varchar),7,2)) as date)
end as sls_order_dt_formatted
from datawarehouse.bronze.crm_sales_details


select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case
	when sls_order_dt is null or len(sls_order_dt) > 8 then NULL
	else try_cast(concat(left(sls_order_dt,4),'-',substring(sls_order_dt,5,2),'-',substring(sls_order_dt,7,2)) as date)
end as sls_order_dt,
case
	when sls_ship_dt is null or len(sls_ship_dt) > 8 then NULL
	else try_cast(concat(left(sls_ship_dt,4),'-',substring(sls_ship_dt,5,2),'-',substring(sls_ship_dt,7,2)) as date)
end as sls_ship_dt,
case
	when sls_due_dt is null or len(sls_due_dt) > 8 then NULL
	else try_cast(concat(left(sls_due_dt,4),'-',substring(sls_due_dt,5,2),'-',substring(sls_due_dt,7,2)) as date)
end as sls_due_dt
from datawarehouse.bronze.crm_sales_details





select
*
from datawarehouse.bronze.crm_sales_details;



use datawarehouse;


select *
from INFORMATION_SCHEMA.COLUMNS
where TABLE_SCHEMA = 'bronze' and TABLE_NAME = 'crm_sales_details'




