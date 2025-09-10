create or alter procedure silver_data_load as 
begin

declare @start_time datetime, @end_time datetime, @store_procedure_start_time datetime, @store_procedure_end_time datetime;

print 'Silver Data Load Store Procedure Started';

	print '--------------started with crm_cust_info table ----------------------------------------';

	set @start_time = getdate();

	truncate table datawarehouse.silver.crm_cust_info;

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

	set @end_time = getdate();

	print 'Time taken to load data into crm_cust_info table is ' + cast(datediff(second,@start_time,@end_time) as varchar);

-----------------------------------------------------------------------------------------------------

	print '--------------started with crm_prd_info table ----------------------------------------';

	set @start_time = getdate();

	truncate table datawarehouse.silver.crm_prd_info;

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

	set @end_time = getdate();

	print 'Time taken to load data into crm_prd_info table is ' + cast(datediff(second,@start_time,@end_time) as varchar);

-------------------------------------------------------------------------------------------------------

	print '--------------started with crm_sales_details table ----------------------------------------';

	set @start_time = getdate();

	truncate table datawarehouse.silver.crm_sales_details;


	with cleaning_up_cte as (
	select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
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
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
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
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
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

	),

	final_cleaned_up_cte_second as (
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case
		when sls_order_dt is null or len(sls_order_dt) > 8 then dateadd(day,-4,try_cast(concat(left(sls_ship_dt,4),'-',substring(sls_ship_dt,5,2),'-',substring(sls_ship_dt,7,2)) as date))
		else try_cast(concat(left(sls_order_dt,4),'-',substring(sls_order_dt,5,2),'-',substring(sls_order_dt,7,2)) as date)
	end as sls_order_dt,
	case
		when sls_ship_dt is null or len(sls_ship_dt) > 8 then dateadd(day,-4,try_cast(concat(left(sls_due_dt,4),'-',substring(sls_due_dt,5,2),'-',substring(sls_due_dt,7,2)) as date))
		else try_cast(concat(left(sls_ship_dt,4),'-',substring(sls_ship_dt,5,2),'-',substring(sls_ship_dt,7,2)) as date)
	end as sls_ship_dt,
	case
		when sls_due_dt is null or len(sls_due_dt) > 8 then dateadd(day,4,try_cast(concat(left(sls_ship_dt,4),'-',substring(sls_ship_dt,5,2),'-',substring(sls_ship_dt,7,2)) as date))
		else try_cast(concat(left(sls_due_dt,4),'-',substring(sls_due_dt,5,2),'-',substring(sls_due_dt,7,2)) as date)
	end as sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	from final_cleaned_up_cte)

	insert into datawarehouse.silver.crm_sales_details
	(sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select *
	from final_cleaned_up_cte_second;



	set @end_time = getdate();

	print 'Time taken to load data into crm_sales_details table is ' + cast(datediff(second,@start_time,@end_time) as varchar);


-----------------------------------------------------------------------------------------------------

	print '--------------started with erp_CUST_AZ12 table ----------------------------------------';

	set @start_time = getdate();

	truncate table datawarehouse.silver.erp_CUST_AZ12;

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


	set @end_time = getdate();

	print 'Time taken to load data into erp_CUST_AZ12 table is ' + cast(datediff(second,@start_time,@end_time) as varchar);
	
-------------------------------------------------------------------------------------------------------

	print '--------------started with erp_LOC_A101 table ----------------------------------------';

	set @start_time = getdate();

	truncate table datawarehouse.silver.erp_LOC_A101;


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


	set @end_time = getdate();

	print 'Time taken to load data into erp_LOC_A101 table is ' + cast(datediff(second,@start_time,@end_time) as varchar);

-------------------------------------------------------------------------------------------------------

	print '--------------started with erp_PX_CAT_G1V2 table ----------------------------------------';

	set @start_time = getdate();

	truncate table datawarehouse.silver.erp_PX_CAT_G1V2;

	insert into datawarehouse.silver.erp_PX_CAT_G1V2
	(
	ID,
	CAT,
	SUBCAT,
	MAINTENANCE
	)
	select *
	from datawarehouse.bronze.erp_PX_CAT_G1V2;


	set @end_time = getdate();

	print 'Time taken to load data into erp_PX_CAT_G1V2 table is ' + cast(datediff(second,@start_time,@end_time) as varchar);


end

exec silver_data_load;

select *
from datawarehouse.silver.crm_cust_info;

select *
from datawarehouse.silver.crm_prd_info;

select *
from datawarehouse.silver.crm_sales_details;

select *
from datawarehouse.silver.erp_CUST_AZ12;


select *
from datawarehouse.silver.erp_LOC_A101;

select *
from datawarehouse.silver.erp_PX_CAT_G1V2;

