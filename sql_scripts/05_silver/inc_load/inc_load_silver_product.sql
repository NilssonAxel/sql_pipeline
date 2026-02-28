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

CREATE OR ALTER PROCEDURE silver.load_silver_product AS
BEGIN
/*###############################################################################################
Script:        DataWarehouse – Load data into silver.product
Author:        Axel Nilsson
Description:   This script loads data from bronze.crm_prd_info.
               The data is inserted into silver.product.

Input:

   * None

Output:

   - Loads data in to the following:

       * Table___silver.product

Dependencies:

   * Database: DataWarehouse
   * Schema: silver
   * Schema: bronze
   * Table silver.product
   * Table: bronze.crm_prd_info
   * SP: bronze.load_bronze
   * Initial load made into: silver.product

Notes:

   * Idempotent
   * Run this procedure after bronze.load_bronze
   * To use the stored procedure, type: EXEC silver.load_silver_product
   
###############################################################################################*/
	
	DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

	DECLARE	@last_load_silver_product DATETIME2 = (SELECT MAX(modified_date) FROM silver.product);

	DECLARE @rows_inserted INT = 0, @rows_updated INT = 0;
	DECLARE @rows_effected TABLE (type VARCHAR(10));

	SET @script_time_started = SYSDATETIME();

	SET NOCOUNT ON;

	BEGIN TRY
	BEGIN TRAN;
		/********************************************************
		Inserts data to table 'silver.product'
		********************************************************/

		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: product';
		PRINT '-------------------------------------------';

		WITH crm_prd AS (
		-- Transforms bronze.crm_prd_info and makes sure the PK is valid
		SELECT 
			prd_id,
			SUBSTRING(UPPER(TRIM(prd_key)), 7, LEN(prd_key)) AS prd_key,
			UPPER(LEFT(TRIM(prd_key), 5)) AS cat_key,
			TRIM(prd_nm) AS prd_name,
			TRY_CAST(prd_cost AS DECIMAL(10,2)) AS prd_cost,
			CASE prd_line
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Sport'
				WHEN 'M' THEN 'Mountain'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A'
			END AS prd_line,
			CASE
				WHEN TRY_CAST(prd_start_dt AS DATE) IS NULL THEN SYSDATETIME() --@script_time_started
				ELSE TRY_CAST(prd_start_dt AS DATE)
			END AS valid_from,
			-- Below! Uses prd_key to only keep the current price for every product. This can cause inconsistancies between product price and prices in crm_sales_details!
			ROW_NUMBER() OVER (PARTITION BY prd_key ORDER BY load_date DESC, prd_start_dt DESC) AS dupes
		FROM bronze.crm_prd_info
		WHERE 
			load_date > @last_load_silver_product AND
			TRY_CAST(prd_id AS INT) IS NOT NULL
		),
		prd_hashed AS (
		SELECT
			prd_id, 
			prd_key, 
			cat_key,
			prd_name,
			prd_cost,
			prd_line,
			valid_from,
			@script_time_started AS modified_date,
			HASHBYTES('SHA2_256', CONCAT_WS( '|',					-- row_hash is for @rows_updated, not for query optimization
				prd_id,
				prd_key,
				cat_key,
				prd_name,
				prd_cost,
				prd_line
				)) AS row_hash
		FROM crm_prd 
		WHERE dupes = 1
		)

		MERGE silver.product AS tgt
		USING prd_hashed AS src
		    ON tgt.prd_key = src.prd_key							-- Uses prd_key to avoid duplicates due to scd0 with new (prd_id) from source
		
		WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN			-- row_hash is for @rows_updated, not for query optimization
		    UPDATE SET
		        tgt.prd_id = src.prd_id,
		        tgt.cat_key = src.cat_key,
				tgt.prd_name = src.prd_name,
				tgt.prd_cost = src.prd_cost,
				tgt.prd_line = src.prd_line,
				tgt.valid_from = src.valid_from,
				tgt.modified_date = src.modified_date,
				tgt.row_hash = src.row_hash
		
		WHEN NOT MATCHED BY TARGET THEN
		    INSERT (prd_id, prd_key, cat_key, prd_name, prd_cost, prd_line, valid_from, modified_date, row_hash)
		    VALUES (src.prd_id, src.prd_key, src.cat_key, src.prd_name, src.prd_cost, src.prd_line, src.valid_from, modified_date, src.row_hash)
		
		OUTPUT $action INTO @rows_effected(type);

		SELECT
		    @rows_inserted = COALESCE(SUM(CASE WHEN type = 'INSERT' THEN 1 ELSE 0 END), 0),
		    @rows_updated  = COALESCE(SUM(CASE WHEN type = 'UPDATE' THEN 1 ELSE 0 END), 0)
		FROM @rows_effected;
		
		PRINT '>> Data successfully loaded into product <<';

		SET @script_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('silver.load_silver_product', 'bronze.crm_prd_info', 'silver.product', @rows_inserted, @rows_updated, 0 , @script_time_started, @script_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: silver.product';
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
		PRINT 'Error occured during loading into table: silver.product';
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
PRINT 'Name: silver.load_silver_product'