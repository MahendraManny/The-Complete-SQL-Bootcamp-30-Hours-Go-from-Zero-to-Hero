
------------------------------ Creation of Tables ---------------------------------------------------------


use datawarehouse;


if object_id('bronze.crm_cust_info','U') is not null
	drop table bronze.crm_cust_info;

create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(30),
cst_firstname nvarchar(30),
cst_lastname nvarchar(30),
cst_marital_status nvarchar(5),
cst_gndr nvarchar(4),
cst_create_date nvarchar(50),
);


if object_id('bronze.crm_prd_info','U') is not null
	drop table bronze.crm_prd_info;

create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(30),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(10),
prd_start_dt nvarchar(50),
prd_end_dt nvarchar(50),
);



if object_id('bronze.crm_sales_details','U') is not null
	drop table bronze.crm_sales_details;

create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt nvarchar(50),
sls_ship_dt nvarchar(50),
sls_due_dt nvarchar(50),
sls_sales int,
sls_quantity int,
sls_price int,
);


if object_id('bronze.erp_CUST_AZ12','U') is not null
	drop table bronze.erp_CUST_AZ12;
	
create table bronze.erp_CUST_AZ12(
CID nvarchar(50),
BDATE nvarchar(50),
GEN nvarchar(20),
);


if object_id('bronze.erp_LOC_A101') is not null
	drop table bronze.erp_LOC_A101;

create table bronze.erp_LOC_A101(
CID nvarchar(50),
CNTRY nvarchar(50),
);



if object_id('bronze.erp_PX_CAT_G1V2') is not null
	drop table bronze.erp_PX_CAT_G1V2;

create table bronze.erp_PX_CAT_G1V2(
ID nvarchar(25),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(50),
);