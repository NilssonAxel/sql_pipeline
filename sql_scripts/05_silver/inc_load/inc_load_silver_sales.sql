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

CREATE OR ALTER PROCEDURE silver.load_silver_sales AS
BEGIN
/*###############################################################################################
Script:        DataWarehouse – Load data into silver.sales
Author:        Axel Nilsson
Description:   This script loads data from bronze.crm_sales_details.
               The data is inserted into silver.sales.

Input:

   * None

Output:

   - Loads data in to the following:

       * Table___silver.sales
	   * Column__Status 0 = Untested | 1 = Passed | 2 = Failed

Dependencies:

   * Database: DataWarehouse
   * Schema: silver
   * Schema: bronze
   * Table silver.sales
   * Table: bronze.crm_sales_details
   * SP: bronze.load_bronze
   * Initial load made into: silver.sales

Notes:

   * This procedure will accept duplicates, but will flag them as faulty (status = 2)
   * Idempotent
   * Append Only
   * Run this procedure after bronze.load_bronze
   * To use the stored procedure, type: EXEC silver.load_silver_sales

###############################################################################################*/
	
	DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

	DECLARE	@last_load_sales DATETIME2 = (SELECT MAX(load_date) FROM silver.sales);

	DECLARE @rows_inserted INT = 0

	SET @script_time_started = SYSDATETIME();

	SET NOCOUNT ON;

	BEGIN TRY
	BEGIN TRAN;		
		
		/********************************************************
		Inserts data to table 'silver.sales'
		********************************************************/
		
		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: sales';
		PRINT '-------------------------------------------';

		-- Transforms bronze.sales and makes sure the PK is valid
		WITH crm_sales AS (
		SELECT 
			TRIM(sls_ord_num) AS sls_ord_num,
			TRIM(sls_prd_key) AS sls_prd_key,
			TRY_CAST(sls_cust_id AS INT) AS sls_cust_id,
			TRY_CAST(sls_order_dt AS DATE) AS sls_order_dt,
			TRY_CAST(sls_ship_dt AS DATE) AS sls_ship_dt,
			TRY_CAST(sls_due_dt AS DATE) AS sls_due_dt,
			TRY_CAST(ABS(sls_sales) AS DECIMAL(10,2)) AS sls_sales,		-- ABS added due to occasional system problems (hopefully) with minus signs before values
			TRY_CAST(sls_quantity AS INT) AS sls_quantity,
			TRY_CAST(ABS(sls_price) AS DECIMAL(10,2)) AS sls_price,		-- ABS added due to occasional system problems (hopefully) with minus signs before values
			ROW_NUMBER() OVER (PARTITION BY sls_ord_num, sls_prd_key ORDER BY load_date DESC) AS dupes
		FROM bronze.crm_sales_details
		WHERE 
			load_date > @last_load_sales AND
			TRIM(sls_ord_num) IS NOT NULL AND
			TRIM(sls_prd_key) IS NOT NULL
		)

		INSERT INTO silver.sales
		SELECT
			sls_ord_num, 
			sls_prd_key, 
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price,
			@script_time_started AS load_date,
			0 AS status			-- 0 = Untested | 1 = Passed | 2 = Failed
		FROM crm_sales
		WHERE dupes = 1

		SET @rows_inserted = @@ROWCOUNT

		PRINT '>> Data successfully loaded into sales <<';

		SET @script_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('silver.load_silver_sales', 'bronze.crm_sales_details', 'silver.sales', @rows_inserted, 0, 0, @script_time_started, @script_time_ended);
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: silver.sales';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows_inserted AS VARCHAR(25));

		/********************************************************
		Tests data in table 'silver.sales'
		********************************************************/
		-- 0 = Untested | 1 = Passed | 2 = Failed

		PRINT ''
		PRINT '>> Testing new data...'

		-- Test price, quanity, sales
		UPDATE s
		SET status = 2
		FROM silver.sales s
		WHERE 
			status = 0 AND 
			CAST(sls_quantity AS INT) * sls_price <> sls_sales;

		-- Test overlapping dates
		UPDATE s
		SET status = 2
		FROM silver.sales s
		WHERE 
			status = 0 AND 
			(sls_order_dt > sls_ship_dt OR
			sls_order_dt > sls_due_dt OR
			sls_ship_dt > sls_due_dt);

		-- Test null-values
		UPDATE s
		SET status = 2
		FROM silver.sales s
		WHERE 
			status = 0 AND
			(sls_ord_num IS NULL OR
			sls_prd_key IS NULL OR
			sls_cust_id IS NULL OR
			sls_order_dt IS NULL OR
			sls_ship_dt IS NULL OR
			sls_due_dt IS NULL OR
			sls_quantity IS NULL);

		-- Test duplicates
		UPDATE s
		SET status = 2
		FROM silver.sales s
		JOIN (
		SELECT sls_ord_num, sls_prd_key
		FROM silver.sales
		GROUP BY sls_ord_num, sls_prd_key
		HAVING COUNT(*) > 1
		) dupes ON
			s.sls_ord_num = dupes.sls_ord_num AND
			s.sls_prd_key = dupes.sls_prd_key
		WHERE status = 0;

		-- Set rest to status = Passed
		UPDATE s
		SET status = 1
		FROM silver.sales s
		WHERE status = 0;

		PRINT '>> New data tested <<'

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
		PRINT 'Error occured during loading into table: silver.sales';
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
PRINT 'Name: silver.load_silver_sales'