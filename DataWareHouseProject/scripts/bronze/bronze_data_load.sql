create or alter procedure load_bronze_layer_data 
as
begin

	declare @start_time datetime,@end_time datetime,@batch_start_time datetime,@batch_end_time datetime

	print '-----------------loading silver layer data'

------------------------------------First Table ----------------------------------------
	set @batch_start_time = getdate()
	set @start_time = GETDATE()

	truncate table datawarehouse.bronze.crm_cust_info
	print 'truncated bronze.crm_cust_info successfully'

	bulk insert datawarehouse.bronze.crm_cust_info
	from 'C:\temp\datasets\source_crm\cust_info.csv'
	with 
	(firstrow=2,
	fieldterminator=','
	);
	print 'loading data into bronze.crm_cust_info successfully'	
	print '-------------------------Moving to the next table-----------------------------'

	set @end_time = getdate()

	print 'Total time taken to complete loading data into the first table is ' + cast(datediff(second,@end_time,@start_time) as nvarchar(50)) + ' seconds'


------------------------------------- Done with First Table --------------------------------
-------------------------------------- Second Table ----------------------------------------
	
	set @start_time = getdate()

	truncate table datawarehouse.bronze.crm_prd_info
	print 'truncated bronze.crm_prd_info successfully'

	bulk insert datawarehouse.bronze.crm_prd_info
	from 'C:\temp\datasets\source_crm\prd_info.csv'
	with 
	(firstrow=2,
	fieldterminator=','
	);
	print 'loading data into bronze.crm_prd_info successfully'
	print '-------------------------Moving to the next table'

	set @end_time = getdate()

	print 'Total time taken to complete loading data into the second table is ' + cast(datediff(second,@end_time,@start_time) as nvarchar(50)) + ' seconds'



-----------------------------------Done with the Second Table------------------------------
-------------------------------------Third Table -----------------------------------------
	
	set @start_time = getdate()
	
	truncate table datawarehouse.bronze.crm_sales_details;
	print 'truncated bronze.crm_sales_details successfully'


	bulk insert datawarehouse.bronze.crm_sales_details
	from 'C:\temp\datasets\source_crm\sales_details.csv'
	with 
	(firstrow=2,
	fieldterminator=','
	);
	print 'loading data into bronze.crm_sales_details successfully'
	print '-------------------------Moving to the next table'

	set @end_time = getdate()

	print 'Total time taken to complete loading data into the third table is ' + cast(datediff(second,@end_time,@start_time) as nvarchar(50)) + ' seconds'



--------------------------------Done with Third Table ------------------------------------
-------------------------------- Forth Table ----------------------------------------------
	
	set @start_time = getdate()
	
	truncate table datawarehouse.bronze.erp_CUST_AZ12
	print 'truncated bronze.erp_CUST_AZ12 successfully'
	
	bulk insert datawarehouse.bronze.erp_CUST_AZ12
	from 'C:\temp\datasets\source_erp\CUST_AZ12.csv'
	with 
	(firstrow=2,
	fieldterminator=','
	);
	print 'loading data into bronze.erp_CUST_AZ12 successfully'
	print '-------------------------Moving to the next table'

	set @end_time = getdate()

	print 'Total time taken to complete loading data into the forth table is ' + cast(datediff(second,@end_time,@start_time) as nvarchar(50)) + ' seconds'
	   	 
------------------------------- Done with Forth Table
-------------------------------- Fifth Table --------------------------------------------
	
	set @start_time = GETDATE()
	
	truncate table datawarehouse.bronze.erp_LOC_A101
	print 'truncated bronze.erp_LOC_A101 successfully'

	bulk insert datawarehouse.bronze.erp_LOC_A101
	from 'C:\temp\datasets\source_erp\LOC_A101.csv'
	with 
	(firstrow=2,
	fieldterminator=','
	);
	print 'loading data into bronze.erp_LOC_A101 successfully'
	print '-------------------------Moving to the next table'

	set @end_time = getdate()

	print 'Total time taken to complete loading data into the fifth table is ' + cast(datediff(second,@end_time,@start_time) as nvarchar(50)) + ' seconds'



----------------------------------- Done with Fith Table ----------------------------------
-------------------------------------Sixth Table ----------------------------------------------
	
	set @start_time = getdate()

	truncate table datawarehouse.bronze.erp_PX_CAT_G1V2
	print 'truncated bronze.erp_PX_CAT_G1V2 successfully'

	bulk insert datawarehouse.bronze.erp_PX_CAT_G1V2
	from 'C:\temp\datasets\source_erp\PX_CAT_G1V2.csv'
	with 
	(firstrow=2,
	fieldterminator=','
	);
	print 'loading data into bronze.erp_PX_CAT_G1V2 successfully'

	set @end_time = getdate()

	print 'Total time taken to complete loading data into the sixth table is ' + cast(datediff(second,@end_time,@start_time) as nvarchar(50)) + ' seconds'

	set @batch_end_time = getdate()

	print 'Total time taken to execute the procedure is ' + cast(datediff(second,@batch_end_time,@batch_start_time) as nvarchar(50)) + ' seconds'


-------------------------------------- Done with Sixth Table-------------------------------

	
end;


exec load_bronze_layer_data;
