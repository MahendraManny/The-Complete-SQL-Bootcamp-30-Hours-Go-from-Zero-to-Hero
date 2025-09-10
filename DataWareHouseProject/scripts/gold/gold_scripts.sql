create view gold.dim_customers as
select 
row_number() over(order by cst_id) as idx,
a.cst_id as customer_id,
a.cst_key customer_number,
a.cst_firstname as first_name,
a.cst_lastname as last_name,
case
	when a.cst_gndr = 'n/a' and b.GEN != 'n/a' then b.GEN
	when a.cst_gndr = 'n/a' and b.GEN = 'n/a' then a.cst_gndr
	else a.cst_gndr
end as 'Gender',
a.cst_marital_status as marital_status,
b.BDATE as birth_date,
c.CNTRY as country,
a.dwh_create_date
from datawarehouse.silver.crm_cust_info a
left join datawarehouse.silver.erp_CUST_AZ12 b on a.cst_key = b.CID
left join datawarehouse.silver.erp_LOC_A101 c on a.cst_key = c.CID

----------------------------------------------------------------------------------------------------------


create view gold.dim_products as
select 
row_number() over(order by a.prd_id) as idx,
a.prd_id as product_id,
a.cat_id as category_id,
a.prd_key as product_key,
a.prd_nm as product_number,
a.prd_cost as product_cost,
a.prd_line as product_line,
a.prd_start_dt as start_date,
b.CAT as category,
b.SUBCAT as subcategory,
b.MAINTENANCE as maintenance,
a.dwh_create_date
from datawarehouse.silver.crm_prd_info a
left join datawarehouse.silver.erp_PX_CAT_G1V2 b on a.cat_id = b.ID
where a.prd_end_dt is null

-----------------------------------------------------------------------------------------------------

create view gold.fact_sales as
select 
a.sls_ord_num as order_number,
c.product_key as product_key,
b.customer_id as customer_id,
a.sls_order_dt as order_date,
a.sls_ship_dt as shipping_date,
a.sls_due_dt as duer_date,
a.sls_sales as sales_amount,
a.sls_quantity as quantity,
a.sls_price as price
from datawarehouse.silver.crm_sales_details a
left join gold.dim_customers b on a.sls_cust_id = b.customer_id
left join gold.dim_products c on a.sls_prd_key = c.product_key