/********************************************************
Initializing script
********************************************************/
USE DataWarehousev1;
GO

PRINT '**********************************************************'
PRINT 'Switching database. Current database in use: ' + CAST(DB_NAME() AS VARCHAR(50))
PRINT '**********************************************************'
PRINT ''
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
/*###############################################################################################
Script:        DataWarehousev1 – Load data into bronze
Author:        Axel Nilsson
Description:   This script loads data from landing to bronze.
               The data is inserted into tables in the schema: bronze.

Input:

   * None

Output:

   - Loads data in to the following:

       * Table___bronze.crm_cust_info
       * Table___bronze.crm_prd_info
       * Table___bronze.crm_sales_details
       * Table___bronze.erp_cust_az12
       * Table___bronze.erp_loc_a101
       * Table___bronze.erp_px_cat_g1v2

Dependencies:

   * Database: DataWarehousev1
   * Schema: bronze
   * Schema: landing
   * Table: landing.crm_cust_info
   * Table: landing.crm_prd_info
   * Table: landing.crm_sales_details
   * Table: landing.erp_cust_az12
   * Table: landing.erp_loc_a101
   * Table: landing.erp_px_cat_g1v2
   * SP: landing.load_landing

Notes:

   * This procedure is not idempotent
   * Make sure this procedure is run after landing.load_landing
   * To use the stored procedure, type: EXEC bronze.load_bronze
   
###############################################################################################*/


	DECLARE @rows INT;
	
	DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;
	DECLARE @batch_time_started DATETIME2, @batch_time_ended DATETIME2;
	
	SET @script_time_started = SYSDATETIME();
	
	SET NOCOUNT ON;

	BEGIN TRY
	BEGIN TRAN;
		PRINT '==========================================================';
		PRINT 'Loading data into bronze tables from CRM landing tables';
		PRINT '==========================================================';
		
		/********************************************************
		Inserts data to table 'bronze.crm_cust_info'
		********************************************************/
		
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: crm_cust_info';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();

		PRINT '>> Loading data from landing...'
		INSERT INTO bronze.crm_cust_info
		SELECT
			 [cst_id]
			,[cst_key]
			,[cst_firstname]
			,[cst_lastname]
			,[cst_marital_status]
			,[cst_gndr]
			,[cst_create_date]
			,@batch_time_started
		FROM landing.crm_cust_info
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';

		SET @batch_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('bronze.load_bronze', 'landing.crm_cust_info', 'bronze.crm_cust_info', @rows, 0, 0, @batch_time_started, @batch_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: crm_cust_info';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));

		/********************************************************
		Inserts data to table 'bronze.crm_prd_info'
		********************************************************/
		
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: bronze.crm_prd_info';
		PRINT '-------------------------------------------';
		
		PRINT '>> Loading data from landing...'
		INSERT INTO bronze.crm_prd_info
		SELECT
			 [prd_id]
			,[prd_key]
			,[prd_nm]
			,[prd_cost]
			,[prd_line]
			,[prd_start_dt]
			,[prd_end_dt]
			,@batch_time_started
		FROM landing.crm_prd_info
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('bronze.load_bronze', 'landing.crm_prd_info', 'bronze.crm_prd_info', @rows, 0, 0, @batch_time_started, @batch_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: bronze.crm_prd_info';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));

		/********************************************************
		Inserts data to table 'bronze.crm_sales_details'
		********************************************************/
		
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: crm_sales_details';
		PRINT '-------------------------------------------';
		
		PRINT '>> Loading data from landing...'
		INSERT INTO bronze.crm_sales_details
		SELECT
			 [sls_ord_num]
			,[sls_prd_key]
			,[sls_cust_id]
			,[sls_order_dt]
			,[sls_ship_dt]
			,[sls_due_dt]
			,[sls_sales]
			,[sls_quantity]
			,[sls_price]
			,@batch_time_started
		FROM landing.crm_sales_details
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('bronze.load_bronze', 'landing.crm_sales_details', 'bronze.crm_sales_details', @rows, 0, 0, @batch_time_started, @batch_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: bronze.crm_sales_details';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));

		/********************************************************
		Inserts source data to table 'bronze.ERP_CUST_AZ12'
		********************************************************/
	
		PRINT '';
		PRINT '==========================================================';
		PRINT 'Loading data into bronze tables from ERP landing tables';
		PRINT '==========================================================';

		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: erp_cust_az12';
		PRINT '-------------------------------------------';

		SET @batch_time_started = SYSDATETIME();

		PRINT '>> Loading data from landing...'
		INSERT INTO bronze.erp_cust_az12
		SELECT
			 [CID]
			,[BDATE]
			,[GEN]
			,@batch_time_started
		FROM landing.erp_cust_az12
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('bronze.load_bronze', 'landing.erp_cust_az12', 'bronze.erp_cust_az12', @rows, 0, 0, @batch_time_started, @batch_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: erp_cust_az12';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));

		/********************************************************
		Inserts source data to table 'bronze.erp_loc_a101'
		********************************************************/
	
		PRINT '';
		PRINT '==========================================================';
		PRINT 'Loading data into bronze tables from ERP landing tables';
		PRINT '==========================================================';

		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: erp_loc_a101';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();

		PRINT '>> Loading data from landing...'
		INSERT INTO bronze.erp_loc_a101
		SELECT
			 [CID]
			,[CNTRY]
			,@batch_time_started
		FROM landing.erp_loc_a101
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('bronze.load_bronze', 'landing.erp_loc_a101', 'bronze.erp_loc_a101', @rows, 0, 0, @batch_time_started, @batch_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: erp_loc_a101';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));

		/********************************************************
		Inserts source data to table 'bronze.erp_px_cat_g1v2'
		********************************************************/
	
		PRINT '';
		PRINT '==========================================================';
		PRINT 'Loading data into bronze tables from ERP landing tables';
		PRINT '==========================================================';

		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: erp_px_cat_g1v2';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();

		PRINT '>> Loading data from landing...'
		INSERT INTO bronze.erp_px_cat_g1v2
		SELECT
			 [ID]
			,[CAT]
			,[SUBCAT]
			,[MAINTENANCE]
			,@batch_time_started
		FROM landing.erp_px_cat_g1v2
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('bronze.load_bronze', 'landing.erp_px_cat_g1v2', 'bronze.erp_px_cat_g1v2', @rows, 0, 0, @batch_time_started, @batch_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: erp_px_cat_g1v2';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));

		/********************************************************
		Finishing script
		********************************************************/
	
		SET NOCOUNT OFF;
		
		COMMIT TRAN;
		SET @script_time_ended = SYSDATETIME();
	    PRINT '';
		PRINT '-------------------------------------------';
		PRINT 'The script ended successfully';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
	END TRY
	BEGIN CATCH
		ROLLBACK TRAN;
		PRINT '================================================================';
		PRINT 'Error occured during loading into tables for schema: landing';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
		PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
		PRINT 'No data was inserted to any of the tables'
		PRINT '================================================================';
	END CATCH
END;
GO

PRINT 'The creation script of the stored procedure has been executed'
PRINT 'Database: DataWarehousev1'
PRINT 'Name: bronze.load_bronze'