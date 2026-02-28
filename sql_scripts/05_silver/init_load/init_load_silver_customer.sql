/*###############################################################################################
Script:        DataWarehouse – Initial load into silver.customer
Author:        Axel Nilsson
Description:   This script loads data from bronze.erp_loc_a101, erp_cust_az12, bronze.crm_cust_info.
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
   * Table: silver.customer
   * Table: bronze.crm_cust_info
   * Table: bronze.erp_cust_az12
   * Table: bronze.erp_loc_a101
   * SP: bronze.load_bronze

Notes:

   * Idempotent
   * Run this script when table silver.customer is empty
   
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
	Inserts data to table 'silver.customer'
	********************************************************/
	IF NOT EXISTS (SELECT 1 FROM silver.customer)
	BEGIN
		PRINT '-------------------------------------------';
		PRINT 'Loading initial data into table: customer';
		PRINT '-------------------------------------------';

		DROP INDEX IF EXISTS crix_customer_cstid ON silver.customer;

		-- Transforms bronze.erp_loc_a101 and makes sure the PK is valid
		WITH erp_loc AS (
		SELECT 
			TRY_CAST(SUBSTRING(TRIM(cid), 7, LEN(cid)) AS INT) AS cid,	-- Creates a INT to match crm_cust_info(cst_id) for join
			TRIM(cid) AS ckey,
			CASE UPPER(TRIM(cntry))
				WHEN 'UNITED KINGDOM' THEN 'UK'
				WHEN 'UNITED STATES' THEN 'USA'
				WHEN 'US' THEN 'USA'
				WHEN 'DE' THEN 'Germany'
				WHEN NULL THEN 'N/A'
				ELSE TRIM(cntry)
			END AS cntry,
			ROW_NUMBER() OVER (PARTITION BY TRIM(cid) ORDER BY load_date DESC) AS dupes
		FROM bronze.erp_loc_a101
		WHERE TRIM(cid) IS NOT NULL
		),
		erp_cust AS (
		-- Transforms bronze.erp_cust_az12 and makes sure the PK is valid
		SELECT 
			-- Creates a INT to match crm_cust_info(cst_id) for join
			TRY_CAST(
				CASE
					WHEN TRIM(cid) LIKE 'NAS%' THEN SUBSTRING(TRIM(cid), 9, LEN(cid))
					ELSE SUBSTRING(TRIM(cid), 6, LEN(cid))
				END AS INT
			) AS cid,										
			TRIM(cid) AS ckey,
			TRY_CAST(bdate AS DATE) AS bdate,
			CASE 
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE NULL
			END AS gen,
			ROW_NUMBER() OVER (PARTITION BY TRIM(cid) ORDER BY load_date DESC) AS dupes
		FROM bronze.erp_cust_az12
		WHERE TRIM(cid) IS NOT NULL
		),
		crm_cust AS (
		-- Transforms bronze.crm_cust_info and makes sure the PK is valid
		SELECT 
			TRY_CAST(cst_id AS INT) AS cst_id,
			UPPER(TRIM(cst_key)) AS cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE
				WHEN UPPER(TRIM(cst_marital_status)) IN ('S', 'SINGLE') THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) IN ('M', 'MARRIED') THEN 'Married'
				ELSE 'N/A'
			END AS cst_marital_status,
			CASE
				WHEN UPPER(TRIM(cst_gndr)) IN ('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) IN ('F', 'FEMALE') THEN 'Female'
				ELSE NULL
			END AS cst_gndr,
			CASE 
				WHEN cst_create_date > @script_time_started THEN @script_time_started
				ELSE TRY_CAST(cst_create_date AS DATE)
			END AS cst_create_date,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY load_date DESC, cst_create_date DESC) AS dupes
		FROM bronze.crm_cust_info
		WHERE TRY_CAST(cst_id AS INT) IS NOT NULL
		),
		cust_merged AS (
		SELECT
			cc.cst_id, 
			cc.cst_key, 
			cc.cst_firstname,
			cc.cst_lastname,
			COALESCE(cc.cst_marital_status, 'N/A') AS cst_marital_status,
			COALESCE(cc.cst_gndr, ec.gen, 'N/A') AS cst_gender,		-- Gender from crm system have priority
			COALESCE(el.cntry, 'N/A') AS cst_country,
			ec.bdate AS cst_birthdate,								-- Nulls are accepted
			cc.cst_create_date,
			@script_time_started AS modified_date
		FROM crm_cust cc
		LEFT JOIN erp_cust ec ON cc.cst_id = ec.cid AND ec.dupes = 1
		LEFT JOIN erp_loc el ON cc.cst_id = el.cid AND el.dupes = 1
		WHERE cc.dupes = 1
		),
		cust_merged_hashed AS (
		SELECT
			cst_id, 
			cst_key, 
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gender,
			cst_country,
			cst_birthdate,
			cst_create_date,
			modified_date,
			HASHBYTES('SHA2_256', CONCAT_WS( '|',					-- row_hash is for @rows_updated, not for query optimization
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gender,
				cst_country,
				cst_birthdate,
				cst_create_date
				)) AS row_hash
		FROM cust_merged
		)

		INSERT INTO silver.customer (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_country, cst_birthdate, cst_create_date, modified_date, row_hash)
		SELECT
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gender, 
			cst_country, 
			cst_birthdate, 
			cst_create_date, 
			@script_time_started, 
			row_hash
		FROM cust_merged_hashed

		SET @rows_inserted = @@ROWCOUNT

		CREATE CLUSTERED INDEX crix_customer_cstid ON silver.customer(cst_id)

		PRINT '>> Data successfully loaded into customer <<';

		SET @script_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('silver.initial_load_silver_customer', 'bronze.crm_cust_info, bronze.erp_cust_az12, bronze.erp_loc_a101', 'silver.customer', @rows_inserted, 0, 0 , @script_time_started, @script_time_ended);	-- Source is open for improvements
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: silver.customer';
		PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
		PRINT 'Rows inserted: ' + CAST(@rows_inserted AS VARCHAR(25));
	END
	ELSE 
		PRINT '>> Data already exists in table silver.customer'

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
	PRINT 'Error occured during loading into table: silver.customer';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
	PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
	PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
	PRINT 'No data was inserted to any of the tables'
	PRINT '================================================================';
END CATCH