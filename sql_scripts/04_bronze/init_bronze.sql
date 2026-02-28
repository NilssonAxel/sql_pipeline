/*###############################################################################################
Script:        DataWarehousev1 – Table setup for schema: bronze
Author:        Axel Nilsson
Description:   This script creates tables for the schema bronze.
               Only in case database named 'DataWarehousev1' exists.

Input:

   * None

Output:

   - Creates the following:

       * Table___bronze.crm_cust_info
       * Table___bronze.crm_prd_info
       * Table___bronze.crm_sales_details
       * Table___bronze.erp_cust_az12
       * Table___bronze.erp_loc_a101
       * Table___bronze.erp_px_cat_g1v2

Dependencies:

   * Database: DataWarehousev1
   * Schema: bronze

Notes:

   * Idempotent

###############################################################################################*/

/********************************************************
Initializing script
********************************************************/

USE DataWarehousev1;

PRINT '**********************************************************'
PRINT 'Switching database. Current database in use: ' + CAST(DB_NAME() AS VARCHAR(50))
PRINT '**********************************************************'
PRINT ''

DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

SET @script_time_started = SYSDATETIME();
PRINT '==========================================';
PRINT 'Creating tables for schema: bronze';
PRINT '==========================================';

BEGIN TRY	
	/********************************************************
	Create table 'crm_cust_info' in schema 'bronze'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_cust_info' AND schema_id = SCHEMA_ID('bronze'))
	BEGIN
		PRINT ''
		PRINT '>> Creating crm_cust_info...';
	
		CREATE TABLE bronze.crm_cust_info
		(
			cst_id NVARCHAR(255),
			cst_key NVARCHAR(255),
			cst_firstname NVARCHAR(255),
			cst_lastname NVARCHAR(255),
			cst_marital_status NVARCHAR(255),
			cst_gndr NVARCHAR(255),
			cst_create_date NVARCHAR(255),
			load_date DATETIME2(0)
		);
		CREATE CLUSTERED INDEX crix_crmcustinfo_loaddate ON bronze.crm_cust_info(load_date)

		PRINT '>> Table crm_cust_info created successfully <<';
	END
	ELSE
		PRINT '>> Table crm_cust_info already exists';
	
	/********************************************************
	Create table 'crm_prd_info' in schema 'bronze'
	********************************************************/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_prd_info' AND schema_id = SCHEMA_ID('bronze'))
	BEGIN
		PRINT '';
		PRINT '>> Creating crm_prd_info';
	
		CREATE TABLE bronze.crm_prd_info
		(
			prd_id NVARCHAR(255),
			prd_key NVARCHAR(255),
			prd_nm NVARCHAR(255),
			prd_cost NVARCHAR(255),
			prd_line NVARCHAR(255),
			prd_start_dt NVARCHAR(255),
			prd_end_dt NVARCHAR(255),
			load_date DATETIME2(0)
		);
		CREATE CLUSTERED INDEX crix_crmprdinfo_loaddate ON bronze.crm_prd_info(load_date)

		PRINT '>> Table crm_prd_info created successfully <<';
	END
	ELSE
		PRINT '>> Table crm_prd_info already exists';
	
	/********************************************************
	Create table 'crm_sales_details' in schema 'bronze'
	********************************************************/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'crm_sales_details' AND schema_id = SCHEMA_ID('bronze'))
	BEGIN
		PRINT '';
		PRINT '>> Creating crm_sales_details...';
	
		CREATE TABLE bronze.crm_sales_details
		(
			sls_ord_num NVARCHAR(255),
			sls_prd_key NVARCHAR(255),
			sls_cust_id NVARCHAR(255),
			sls_order_dt NVARCHAR(255),
			sls_ship_dt NVARCHAR(255),
			sls_due_dt NVARCHAR(255),
			sls_sales NVARCHAR(255),
			sls_quantity NVARCHAR(255),
			sls_price NVARCHAR(255),
			load_date DATETIME2(0)
		);
		CREATE CLUSTERED INDEX crix_crmsalesdetails_loaddate ON bronze.crm_sales_details(load_date)
	
		PRINT '>> Table crm_sales_details created successfully <<';
	END
	ELSE
		PRINT '>> Table crm_sales_details already exists';
	
	/********************************************************
	Create table 'erp_cust_az12' in schema 'bronze'
	********************************************************/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_cust_az12' AND schema_id = SCHEMA_ID('bronze'))
	BEGIN
		PRINT '';
		PRINT '>> Creating erp_cust_az12...';
	
		CREATE TABLE bronze.erp_cust_az12
		(
			cid NVARCHAR(255),
			bdate NVARCHAR(255),
			gen NVARCHAR(255),
			load_date DATETIME2(0)
		);
		CREATE CLUSTERED INDEX crix_erpcustaz12_loaddate ON bronze.erp_cust_az12(load_date)

		PRINT '>> Table erp_cust_az12 created successfully <<';
	END
	ELSE
		PRINT '>> Table erp_cust_az12 already exists';
	
	/********************************************************
	Create table 'erp_loc_a101' in schema 'bronze'
	********************************************************/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_loc_a101' AND schema_id = SCHEMA_ID('bronze'))
	BEGIN
		PRINT '';
		PRINT '>> Creating erp_loc_a101...';
	
		CREATE TABLE bronze.erp_loc_a101
		(
			cid NVARCHAR(255),
			cntry NVARCHAR(255),
			load_date DATETIME2(0)
		);
		CREATE CLUSTERED INDEX crix_erploca101_loaddate ON bronze.erp_loc_a101(load_date)
	
		PRINT '>> Table erp_loc_a101 created successfully <<';
	END
	ELSE
		PRINT '>> Table erp_loc_a101 already exists';
	
	/********************************************************
	Create table 'erp_px_cat_g1v2' in schema 'bronze'
	********************************************************/
	
	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'erp_px_cat_g1v2' AND schema_id = SCHEMA_ID('bronze'))
	BEGIN
		PRINT '';
		PRINT '>> Creating erp_px_cat_g1v2...';
	
		CREATE TABLE bronze.erp_px_cat_g1v2
		(
			id NVARCHAR(255),
			cat NVARCHAR(255),
			subcat NVARCHAR(255),
			maintenance NVARCHAR(255),
			load_date DATETIME2(0)
		);
		CREATE CLUSTERED INDEX crix_erppxcatg1v2_loaddate ON bronze.erp_px_cat_g1v2(load_date)
	
		PRINT '>> Table erp_px_cat_g1v2 created successfully <<';
	END
	ELSE
		PRINT '>> Table erp_px_cat_g1v2 already exists';

	/********************************************************
	Finishing script
	********************************************************/

	SET @script_time_ended = SYSDATETIME();
	PRINT '';
	PRINT '-------------------------------------------';
	PRINT 'The script ended successfully';
	PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
END TRY
BEGIN CATCH
	PRINT '================================================================';
	PRINT 'Error occured during creation of tables for schema: bronze';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
	PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
	PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
	PRINT '================================================================';
END CATCH