insert into datawarehouse.silver.erp_PX_CAT_G1V2
(
ID,
CAT,
SUBCAT,
MAINTENANCE
)
select *
from datawarehouse.bronze.erp_PX_CAT_G1V2;
