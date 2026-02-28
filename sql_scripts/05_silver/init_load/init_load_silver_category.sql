/*###############################################################################################
Script:        DataWarehouse – Initial load into silver.category
Author:        Axel Nilsson
Description:   This script loads data from bronze.erp_px_cat_g1v2.
               The data is inserted into silver.category.

Input:

   * None

Output:

   - Loads data in to the following:

       * Table___silver.category

Dependencies:

   * Database: DataWarehouse
   * Schema: silver
   * Schema: bronze
   * Table silver.category
   * Table: bronze.erp_px_cat_g1v2
   * SP: bronze.load_bronze

Notes:

   * Idempotent
   * Run this script when table silver.category is empty
   
###############################################################################################*/

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

DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;
DECLARE @rows_inserted INT = 0

SET @script_time_started = SYSDATETIME();

SET NOCOUNT ON;

BEGIN TRY
BEGIN TRAN;
	/********************************************************
	Inserts data to table 'silver.category'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM silver.category)
	BEGIN	
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: category';
		PRINT '-------------------------------------------';

		DROP INDEX IF EXISTS nrix_category_cat_key ON silver.category;

		-- Transforms bronze.erp_px_cat_g1v2 and makes sure the PK is valid
		WITH erp_cat AS (
		SELECT 
			STUFF(TRIM(id), 3, 1, '-') AS cat_key,
			TRIM(cat) AS cat,
			TRIM(subcat) AS subcat,
			TRIM(maintenance) AS maintenance,
			ROW_NUMBER() OVER (PARTITION BY TRIM(id) ORDER BY load_date DESC) AS dupes
		FROM bronze.erp_px_cat_g1v2
		WHERE TRIM(id) IS NOT NULL
		),
		cat_hashed AS (
		SELECT
			cat_key, 
			cat, 
			subcat,
			maintenance,
			@script_time_started AS modified_date,
			HASHBYTES('SHA2_256', CONCAT_WS( '|',					-- row_hash is for @rows_updated, not for query optimization
				cat_key, 
				cat, 
				subcat,
				maintenance
				)) AS row_hash
		FROM erp_cat 
		WHERE dupes = 1
		)

		 INSERT INTO silver.category (cat_key, cat, subcat, maintenance, modified_date, row_hash)
		 SELECT 
			cat_key, 
			cat, 
			subcat, 
			maintenance, 
			modified_date, 
			row_hash
		FROM cat_hashed

		SET @rows_inserted = @@ROWCOUNT;

		CREATE INDEX nrix_category_cat_key ON silver.category(cat_key);

		PRINT '>> Data successfully loaded into category <<';

		SET @script_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('silver.initial_load_silver_category', 'bronze.erp_px_cat_g1v2', 'silver.category', @rows_inserted, 0, 0 , @script_time_started, @script_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: silver.category';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows_inserted AS VARCHAR(25));
	END
	ELSE 
		PRINT '>> Data already exists in table silver.category'

	/********************************************************
	Finishing script
	********************************************************/

	SET NOCOUNT OFF;
	
	COMMIT TRAN;

    PRINT '';
	PRINT '-------------------------------------------';
	PRINT 'The script ended successfully';
	PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
END TRY
BEGIN CATCH
	ROLLBACK TRAN;
	PRINT '================================================================';
	PRINT 'Error occured during loading into table: silver.category';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
	PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
	PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
	PRINT 'No data was inserted to any of the tables'
	PRINT '================================================================';
END CATCH