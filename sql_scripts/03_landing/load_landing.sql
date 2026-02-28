
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

CREATE OR ALTER PROCEDURE landing.load_landing 
@path_crm VARCHAR(255) = 'C:\path\datasets\source_crm\',
@path_erp VARCHAR(255) = 'C:\path\datasets\source_erp\'
AS
BEGIN

/*###############################################################################################
Script:        DataWarehousev1 – Load data into landing
Author:        Axel Nilsson
Description:   This script bulk loads data from CRM and ERP files.
               The data is inserted into tables in the schema: landing.

Input:

   * None

Output:

   - Loads data in to the following:

       * Table___landing.crm_cust_info
       * Table___landing.crm_prd_info
       * Table___landing.crm_sales_details
       * Table___landing.erp_cust_az12
       * Table___landing.erp_loc_a101
       * Table___landing.erp_px_cat_g1v2

Dependencies:

   * Database: DataWarehousev1
   * Schema: landing
   * Filepath: @path_crm	Stored Procedure Parameter
   * Filepath: @path_erp	Stored Procedure Parameter
   * Filename: @file_name
   * Data from files fitting datatype VARCHAR(255)

Notes:

   * Truncates tables before loading new data
   * Make sure the filepaths for the files are correct in the variables (see dependencies)
   * Make sure the filenames are correct 
	 they can be found at the start of their relative bulk insert statement
   * To use the stored procedure, type: EXEC landing.load_landing

###############################################################################################*/
	
	DECLARE @file_name VARCHAR(55), @sql NVARCHAR(MAX);
	DECLARE @rows INT;
	
	DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;
	DECLARE @batch_time_started DATETIME2, @batch_time_ended DATETIME2;
	
	SET @script_time_started = SYSDATETIME();
	
	SET NOCOUNT ON;
	
	BEGIN TRY
		PRINT '===================================================';
		PRINT 'Loading data into landing tables from CRM files';
		PRINT '===================================================';
		
		/********************************************************
		Inserts source data to table 'landing.crm_cust_info'
		********************************************************/
		SET @file_name = 'cust_info.csv';
		
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: crm_cust_info';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();
		
		PRINT '>> Truncating previous data...';
		TRUNCATE TABLE landing.crm_cust_info;
		PRINT '>> Previous data truncated <<'
		
		PRINT '>> Loading data from file...'
		SET @sql = '
			BULK INSERT landing.crm_cust_info
			FROM ''' + @path_crm + @file_name +'''
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);';
		
		EXEC(@sql);
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();
		PRINT '';
		PRINT 'Table loaded successfully: crm_cust_info';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));
			
		/********************************************************
		Inserts source data to table 'landing.crm_prd_info'
		********************************************************/
		
		SET @file_name = 'prd_info.csv'
			
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: crm_prd_info';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();
		
		PRINT '>> Truncating previous data...';
		TRUNCATE TABLE landing.crm_prd_info;
		PRINT '>> Previous data truncated <<'
		
		PRINT '>> Loading data from file...'
		SET @sql = '
			BULK INSERT landing.crm_prd_info
			FROM ''' + @path_crm + @file_name +'''
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);';
		
		EXEC(@sql);
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();
		PRINT '';
		PRINT 'Table loaded successfully: crm_prd_info';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));
	
		/********************************************************
		Inserts source data to table 'landing.crm_sales_details'
		********************************************************/
		SET @file_name = 'sales_details.csv'
			
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: crm_sales_details';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();
		
		PRINT '>> Truncating previous data...';
		TRUNCATE TABLE landing.crm_sales_details;
		PRINT '>> Previous data truncated <<'
		
		PRINT '>> Loading data from file...'
		SET @sql = '
			BULK INSERT landing.crm_sales_details
			FROM ''' + @path_crm + @file_name +'''
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);';
		
		EXEC(@sql);
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();
		PRINT '';
		PRINT 'Table loaded successfully: crm_sales_details';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));
	
		/********************************************************
		Inserts source data to table 'landing.ERP_CUST_AZ12'
		********************************************************/
	
		PRINT '';
		PRINT '===================================================';
		PRINT 'Loading data into landing tables from ERP files';
		PRINT '===================================================';
	
		SET @file_name = 'CUST_AZ12.csv'
			
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: erp_cust_az12';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();
		
		PRINT '>> Truncating previous data...';
		TRUNCATE TABLE landing.erp_cust_az12;
		PRINT '>> Previous data truncated <<'
		
		PRINT '>> Loading data from file...'
		SET @sql = '
			BULK INSERT landing.erp_cust_az12
			FROM ''' + @path_erp + @file_name +'''
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);';
		
		EXEC(@sql);
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();
		PRINT '';
		PRINT 'Table loaded successfully: erp_cust_az12';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));
	
		/********************************************************
		Inserts source data to table 'landing.ERP_LOC_A101'
		********************************************************/
		SET @file_name = 'loc_a101.csv'
			
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: erp_loc_a101';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();
		
		PRINT '>> Truncating previous data...';
		TRUNCATE TABLE landing.erp_loc_a101;
		PRINT '>> Previous data truncated <<'
		
		PRINT '>> Loading data from file...'
		SET @sql = '
			BULK INSERT landing.erp_loc_a101
			FROM ''' + @path_erp + @file_name +'''
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);';
		
		EXEC(@sql);
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();
		PRINT '';
		PRINT 'Table loaded successfully: erp_loc_a101';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));
	
		/********************************************************
		Inserts source data to table 'landing.ERP_PX_CAT_G1V2'
		********************************************************/
		SET @file_name = 'PX_CAT_G1V2.csv'
			
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: erp_px_cat_g1v2';
		PRINT '-------------------------------------------';
		
		SET @batch_time_started = SYSDATETIME();
		
		PRINT '>> Truncating previous data...';
		TRUNCATE TABLE landing.erp_px_cat_g1v2;
		PRINT '>> Previous data truncated <<'
		
		PRINT '>> Loading data from file...'
		SET @sql = '
			BULK INSERT landing.erp_px_cat_g1v2
			FROM ''' + @path_erp + @file_name +'''
			WITH
			(
				FIRSTROW = 2,
				FIELDTERMINATOR = '','',
				TABLOCK
			);';
		
		EXEC(@sql);
		SET @rows = @@ROWCOUNT
		PRINT '>> New data inserted <<';
		
		SET @batch_time_ended = SYSDATETIME();
		PRINT '';
		PRINT 'Table loaded successfully: erp_px_cat_g1v2';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @batch_time_started, @batch_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows AS VARCHAR(25));
	
		/********************************************************
		Finishing script
		********************************************************/
	
		SET NOCOUNT OFF;
	
		SET @script_time_ended = SYSDATETIME();
	    PRINT '';
		PRINT '-------------------------------------------';
		PRINT 'The script ended successfully';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
	END TRY
	BEGIN CATCH
		PRINT '================================================================';
		PRINT 'Error occured during loading into tables for schema: landing';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
		PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
		PRINT '================================================================';
	END CATCH
END;
GO

PRINT 'The creation script of the stored procedure has been executed'
PRINT 'Database: DataWarehousev1'
PRINT 'Name: landing.load_landing'