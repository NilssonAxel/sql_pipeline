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

CREATE OR ALTER PROCEDURE silver.load_silver_customer AS
BEGIN
/*###############################################################################################
Script:        DataWarehouse – Load data into silver.customer
Author:        Axel Nilsson
Description:   This script loads data from bronze.erp_loc_a101, erp_cust_az12, bronze.crm_cust_info.
               The data is inserted into silver.customer.

Input:

   * None

Output:

   - Loads data in to the following:

       * Table___silver.customer

Dependencies:

   * Database: DataWarehouse
   * Schema: silver
   * Schema: bronze
   * Table: silver.customer
   * Table: bronze.crm_cust_info
   * Table: bronze.erp_cust_az12
   * Table: bronze.erp_loc_a101
   * SP: bronze.load_bronze
   * Initial load made into: silver.customer

Notes:

   * Idempotent
   * Run this procedure after bronze.load_bronze
   * To use the stored procedure, type: EXEC silver.load_silver_customer

###############################################################################################*/
	
	DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

	-- This is dependent on the systems being in sync
	DECLARE @last_load_silver_customer DATETIME2 = (SELECT MAX(modified_date) FROM silver.customer)

	DECLARE @rows_inserted INT = 0, @rows_updated INT = 0;
	DECLARE @rows_effected TABLE (type VARCHAR(10));

	SET @script_time_started = SYSDATETIME();

	SET NOCOUNT ON;

	BEGIN TRY
	BEGIN TRAN;
		/********************************************************
		Inserts data to table 'silver.customer'
		********************************************************/

		PRINT '-------------------------------------------';
		PRINT 'Loading data into table: customer';
		PRINT '-------------------------------------------';

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
		WHERE 
			load_date > @last_load_silver_customer AND
			TRIM(cid) IS NOT NULL
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
		WHERE 
			load_date > @last_load_silver_customer AND
			TRIM(cid) IS NOT NULL
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
		WHERE 
			load_date > @last_load_silver_customer AND
			TRY_CAST(cst_id AS INT) IS NOT NULL
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

		MERGE silver.customer AS tgt
		USING cust_merged_hashed AS src
		    ON tgt.cst_id = src.cst_id
		
		WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN			-- row_hash is for @rows_updated, not for query optimization
		    UPDATE SET
		        tgt.cst_firstname = src.cst_firstname,
		        tgt.cst_lastname = src.cst_lastname,
				tgt.cst_marital_status = src.cst_marital_status,
				tgt.cst_gender = src.cst_gender,
				tgt.cst_country = src.cst_country,
				tgt.cst_birthdate = src.cst_birthdate,
				tgt.cst_create_date = src.cst_create_date,
				tgt.modified_date = src.modified_date,
				tgt.row_hash = src.row_hash
		
		WHEN NOT MATCHED BY TARGET THEN
		    INSERT (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gender, cst_country, cst_birthdate, cst_create_date, modified_date, row_hash)
		    VALUES (src.cst_id, src.cst_key, src.cst_firstname, src.cst_lastname, src.cst_marital_status, src.cst_gender, src.cst_country, src.cst_birthdate, src.cst_create_date, modified_date, src.row_hash)
			
		OUTPUT $action INTO @rows_effected(type);

		SELECT
		    @rows_inserted = COALESCE(SUM(CASE WHEN type = 'INSERT' THEN 1 ELSE 0 END), 0),
		    @rows_updated  = COALESCE(SUM(CASE WHEN type = 'UPDATE' THEN 1 ELSE 0 END), 0)
		FROM @rows_effected;
		
		PRINT '>> Data successfully loaded into customer <<';

		SET @script_time_ended = SYSDATETIME();

		-- Logs the batch into etl.load_log
		PRINT '>> Logging load to etl.load_log...'
		INSERT INTO etl.load_log ([procedure], [source], [target], rows_inserted, rows_updated, rows_deleted, start_time, end_time)
		VALUES ('silver.load_silver_customer', 'bronze.crm_cust_info, bronze.erp_cust_az12, bronze.erp_loc_a101', 'silver.customer', @rows_inserted, @rows_updated, 0 , @script_time_started, @script_time_ended);	-- Source is open for improvements
		PRINT '>> Load logged successfully <<';

		PRINT '';
		PRINT 'Table loaded successfully: silver.customer';
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
END;
GO

PRINT 'The creation script of the stored procedure has been executed'
PRINT 'Database: DataWarehousev1'
PRINT 'Name: silver.load_silver_customer'