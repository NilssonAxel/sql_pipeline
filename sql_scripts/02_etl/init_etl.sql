/*###############################################################################################
Script:        DataWarehousev1 – Table setup for schema: etl
Author:        Axel Nilsson
Description:   This script creates tables for the schema etl.
               Only in case database named 'DataWarehousev1' exists.

Input:

   * None

Output:

   - Creates the following:

       * Table___etl.load_log

Dependencies:

   * Database: DataWarehousev1
   * Schema: etl

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
PRINT 'Creating tables for schema: etl';
PRINT '==========================================';

BEGIN TRY	
	/********************************************************
	Create table 'load_log' in schema 'etl'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'load_log' AND schema_id = SCHEMA_ID('etl'))
	BEGIN
		PRINT ''
		PRINT '>> Creating load_log...';
	
		CREATE TABLE etl.load_log
		(
			[procedure] VARCHAR(50),
			[source] VARCHAR(255),
			[target] VARCHAR(255),
			rows_inserted INT DEFAULT 0,
			rows_updated INT DEFAULT 0,
			rows_deleted INT DEFAULT 0,
			start_time DATETIME2(0),
			end_time DATETIME2(0) DEFAULT SYSDATETIME()
		);
	
		PRINT '>> Table load_log created successfully <<';
	END
	ELSE
		PRINT '>> Table load_log already exists';

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
	PRINT 'Error occured during creation of tables for schema: etl';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
	PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
	PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
	PRINT '================================================================';
END CATCH