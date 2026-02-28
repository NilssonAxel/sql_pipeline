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

CREATE OR ALTER PROCEDURE silver.load_silver_category AS
BEGIN
/*###############################################################################################
Script:        DataWarehouse – Load data into silver.category
Author:        Axel Nilsson
Description:   This script loads data from bronze.erp_px_cat_g1v2.
               The data is inserted into silver.category.

Input:

   * None

Output:

   - Loads data in to the following:

       * Table___silver.product

Dependencies:

   * Database: DataWarehouse
   * Schema: silver
   * Schema: bronze
   * Table silver.category
   * Table: bronze.erp_px_cat_g1v2
   * SP: bronze.load_bronze
   * Initial load made into: silver.category

Notes:

   * Idempotent
   * Run this procedure after bronze.load_bronze
   * To use the stored procedure, type: EXEC silver.load_silver_category
   
###############################################################################################*/
	
	DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

	DECLARE	@last_load_silver_category DATETIME2 = (SELECT MAX(modified_date) FROM silver.category);

	DECLARE @rows_inserted INT = 0, @rows_updated INT = 0;
	DECLARE @rows_effected TABLE (type VARCHAR(10));

	SET @script_time_started = SYSDATETIME();

	SET NOCOUNT ON;

	BEGIN TRY
	BEGIN TRAN;
		/********************************************************
		Inserts data to table 'silver.category'
		********************************************************/
		
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: category';
		PRINT '-------------------------------------------';

		-- Transforms bronze.erp_px_cat_g1v2 and makes sure the PK is valid
		WITH erp_cat AS (
		SELECT 
			STUFF(TRIM(id), 3, 1, '-') AS cat_key,
			TRIM(cat) AS cat,
			TRIM(subcat) AS subcat,
			TRIM(maintenance) AS maintenance,
			ROW_NUMBER() OVER (PARTITION BY TRIM(id) ORDER BY load_date DESC) AS dupes
		FROM bronze.erp_px_cat_g1v2
		WHERE 
			load_date > @last_load_silver_category AND
			TRIM(id) IS NOT NULL
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

		MERGE silver.category AS tgt
		USING cat_hashed AS src
		    ON tgt.cat_key = src.cat_key							
		
		WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN			-- row_hash is for @rows_updated, not for query optimization
		    UPDATE SET
		        tgt.cat = src.cat,
		        tgt.subcat = src.subcat,
				tgt.maintenance = src.maintenance,
				tgt.modified_date = src.modified_date,
				tgt.row_hash = src.row_hash
		
		WHEN NOT MATCHED BY TARGET THEN
		    INSERT (cat_key, cat, subcat, maintenance, modified_date, row_hash)
		    VALUES (src.cat_key, src.cat, src.subcat, src.maintenance, modified_date, src.row_hash)
		
		OUTPUT $action INTO @rows_effected(type);

		SELECT
		    @rows_inserted = COALESCE(SUM(CASE WHEN type = 'INSERT' THEN 1 ELSE 0 END), 0),
		    @rows_updated  = COALESCE(SUM(CASE WHEN type = 'UPDATE' THEN 1 ELSE 0 END), 0)
		FROM @rows_effected;
		
		PRINT '>> Data successfully loaded into category <<';

		SET @script_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('silver.load_silver_category', 'bronze.erp_px_cat_g1v2', 'silver.category', @rows_inserted, @rows_updated, 0 , @script_time_started, @script_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: silver.category';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows_inserted AS VARCHAR(25));
		PRINT 'Rows updated: ' + CAST(@rows_updated AS VARCHAR(25));

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
END;
GO

PRINT 'The creation script of the stored procedure has been executed'
PRINT 'Database: DataWarehousev1'
PRINT 'Name: silver.load_silver_category'