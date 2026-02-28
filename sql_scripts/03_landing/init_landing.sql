USE DataWarehousev1;

DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

SET @script_time_started = SYSDATETIME();
PRINT '==========================================';
PRINT 'Creating tables for schema: landing';
PRINT '==========================================';

BEGIN TRY
/********************************************************
CREATE TABLE 'crm_cust_info' IN SCHEMA 'landing'
********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_cust_info' AND schema_id = SCHEMA_ID('landing'))
	BEGIN
		PRINT '----------------------------------';
		PRINT 'Creating crm_cust_info';
		PRINT '----------------------------------';
	
		CREATE TABLE landing.crm_cust_info
		(
			cst_id NVARCHAR(255),
			cst_key NVARCHAR(255),
			cst_firstname NVARCHAR(255),
			cst_lastname NVARCHAR(255),
			cst_marital_status NVARCHAR(255),
			cst_gndr NVARCHAR(255),
			cst_create_date NVARCHAR(255)
		);
	
		PRINT '>> Table crm_cust_info created successfully <<';
	END
	ELSE
		PRINT '>> Table crm_cust_info already exists';
	
	/*
	========================================================
	CREATE TABLE 'crm_prd_info' IN SCHEMA 'landing'
	========================================================
	*/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_prd_info' AND schema_id = SCHEMA_ID('landing'))
	BEGIN
		PRINT '----------------------------------';
		PRINT 'Creating crm_prd_info';
		PRINT '----------------------------------';
	
		CREATE TABLE landing.crm_prd_info
		(
			prd_id NVARCHAR(255),
			prd_key NVARCHAR(255),
			prd_nm NVARCHAR(255),
			prd_cost NVARCHAR(255),
			prd_line NVARCHAR(255),
			prd_start_dt NVARCHAR(255),
			prd_end_dt NVARCHAR(255)
		);
	
		PRINT '>> Table crm_prd_info created successfully <<';
	END
	ELSE
		PRINT '>> Table crm_prd_info already exists';
	
	/*
	========================================================
	CREATE TABLE 'crm_sales_details' IN SCHEMA 'landing'
	========================================================
	*/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_sales_details' AND schema_id = SCHEMA_ID('landing'))
	BEGIN
		PRINT '----------------------------------';
		PRINT 'Creating crm_sales_details';
		PRINT '----------------------------------';
	
		CREATE TABLE landing.crm_sales_details
		(
			sls_ord_num NVARCHAR(255),
			sls_prd_key NVARCHAR(255),
			sls_cust_id NVARCHAR(255),
			sls_order_dt NVARCHAR(255),
			sls_ship_dt NVARCHAR(255),
			sls_due_dt NVARCHAR(255),
			sls_sales NVARCHAR(255),
			sls_quantity NVARCHAR(255),
			sls_price NVARCHAR(255)
		);
	
		PRINT '>> Table crm_sales_details created successfully <<';
	END
	ELSE
		PRINT '>> Table crm_sales_details already exists';
	
	/*
	========================================================
	CREATE TABLE 'erp_cust_az12' IN SCHEMA 'landing'
	========================================================
	*/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_cust_az12' AND schema_id = SCHEMA_ID('landing'))
	BEGIN
		PRINT '----------------------------------';
		PRINT 'Creating erp_cust_az12';
		PRINT '----------------------------------';
	
		CREATE TABLE landing.erp_cust_az12
		(
			CID NVARCHAR(255),
			BDATE NVARCHAR(255),
			GEN NVARCHAR(255)
		);
	
		PRINT '>> Table erp_cust_az12 created successfully <<';
	END
	ELSE
		PRINT '>> Table erp_cust_az12 already exists';
	
	/*
	========================================================
	CREATE TABLE 'erp_loc_a101' IN SCHEMA 'landing'
	========================================================
	*/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_loc_a101' AND schema_id = SCHEMA_ID('landing'))
	BEGIN
		PRINT '----------------------------------';
		PRINT 'Creating erp_loc_a101';
		PRINT '----------------------------------';
	
		CREATE TABLE landing.erp_loc_a101
		(
			CID NVARCHAR(255),
			CNTRY NVARCHAR(255)
		);
	
		PRINT '>> Table erp_loc_a101 created successfully <<';
	END
	ELSE
		PRINT '>> Table erp_loc_a101 already exists';
	
	/*
	========================================================
	CREATE TABLE 'erp_px_cat_g1v2' IN SCHEMA 'landing'
	========================================================
	*/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_px_cat_g1v2' AND schema_id = SCHEMA_ID('landing'))
	BEGIN
		PRINT '----------------------------------';
		PRINT 'Creating erp_px_cat_g1v2';
		PRINT '----------------------------------';
	
		CREATE TABLE landing.erp_px_cat_g1v2
		(
			ID NVARCHAR(255),
			CAT NVARCHAR(255),
			SUBCAT NVARCHAR(255),
			MAINTENANCE NVARCHAR(255)
		);
	
		PRINT '>> Table erp_px_cat_g1v2 created successfully <<';
	END
	ELSE
		PRINT '>> Table erp_px_cat_g1v2 already exists';
	
	SET @script_time_ended = SYSDATETIME();
	PRINT 'The script ended successfully';
	PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
END TRY
BEGIN CATCH
	PRINT '================================================================';
	PRINT 'Error occured during creation of tables for schema: landing';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
	PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
	PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
	PRINT '================================================================';
END CATCH