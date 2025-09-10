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

select *
from final_cleaned_up_cte_second
where sls_order_dt is null or sls_ship_dt is null or sls_due_dt is null





