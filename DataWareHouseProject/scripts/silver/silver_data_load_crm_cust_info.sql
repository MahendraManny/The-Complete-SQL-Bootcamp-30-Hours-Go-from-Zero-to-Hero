select *
from datawarehouse.silver.crm_cust_info;

insert into datawarehouse.silver.crm_cust_info
(cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case
	when cst_marital_status = 'M' then 'Married'
	when cst_marital_status = 'S' then 'Single'
	else 'n/a'
end as cst_martial_status,
case
	when cst_gndr = 'M' then 'Male'
	when cst_gndr = 'F' then 'Femal'
	else 'n/a'
end as cst_gndr,
cst_create_date	
from
(select *,
row_number() over(partition by cst_id order by cst_create_date desc) as row_index
from datawarehouse.bronze.crm_cust_info
where cst_id is not null) a
where row_index = 1;