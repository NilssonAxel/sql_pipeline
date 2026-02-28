/*###############################################################################################
Script:        DataWarehouse – Table setup for schema: silver
Author:        Axel Nilsson
Description:   This script creates tables for the schema silver.
               Only in case database named 'DataWarehouse' exists.

Input:

   * None

Output:

   - Creates the following:

       * Table___silver.customer
       * Table___silver.product
	   * Table___silver.sales
	   * Table___silver.category

Dependencies:

   * Database: DataWarehouse
   * Schema: silver

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
PRINT 'Creating tables for schema: silver';
PRINT '==========================================';

BEGIN TRY	
	/********************************************************
	Create table 'customer' in schema 'silver'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'customer' AND schema_id = SCHEMA_ID('silver'))
	BEGIN
		PRINT ''
		PRINT '>> Creating customer...';

		CREATE TABLE silver.customer
		(
			cst_id BIGINT,
			cst_key VARCHAR(25),
			cst_firstname NVARCHAR(25),
			cst_lastname NVARCHAR(25),
			cst_marital_status VARCHAR(15),
			cst_gender VARCHAR(15),
			cst_country VARCHAR(50),
			cst_birthdate DATE,
			cst_create_date DATE,
			modified_date DATETIME2(0),
			row_hash BINARY(32)
		);
		CREATE CLUSTERED INDEX crix_customer_cstid ON silver.customer(cst_id)
	
		PRINT '>> Table customer created successfully <<';
	END
	ELSE
		PRINT '>> Table customer already exists';

	/********************************************************
	Create table 'product' in schema 'silver'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'product' AND schema_id = SCHEMA_ID('silver'))
	BEGIN
		PRINT ''
		PRINT '>> Creating product...';

		CREATE TABLE silver.product
		(
			prd_id INT,
			prd_key NVARCHAR(25),
			cat_key NVARCHAR(10),
			prd_name NVARCHAR(50),
			prd_cost DECIMAL(10,2),
			prd_line NVARCHAR(25),
			valid_from DATE,
			modified_date DATETIME2(0),
			row_hash BINARY(32)
		);
		CREATE CLUSTERED INDEX crix_product_prdid ON silver.product(prd_id)
	
		PRINT '>> Table product created successfully <<';
	END
	ELSE
		PRINT '>> Table product already exists';

	/********************************************************
	Create table 'sales' in schema 'silver'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'sales' AND schema_id = SCHEMA_ID('silver'))
	BEGIN
		PRINT ''
		PRINT '>> Creating sales...';

		CREATE TABLE silver.sales
		(
			sls_ord_num VARCHAR(15),
			sls_prd_key NVARCHAR(25),
			sls_cust_id BIGINT,
			sls_order_dt DATE,
			sls_ship_dt DATE,
			sls_due_dt DATE,
			sls_sales DECIMAL(10,2),
			sls_quantity INT,
			sls_price DECIMAL(10,2),
			load_date DATETIME2(0),
			status INT		-- 0 = Untested | 1 = Passed | 2 = Failed
		);
		CREATE INDEX nrix_sales_orderdt ON silver.sales(sls_order_dt)
		CREATE INDEX nrix_sales_prdkey ON silver.sales(sls_prd_key)
		CREATE INDEX nrix_sales_custid ON silver.sales(sls_cust_id)
		CREATE INDEX nrix_sales_status ON silver.sales(status)
	
		PRINT '>> Table sales created successfully <<';
	END
	ELSE
		PRINT '>> Table sales already exists';


	/********************************************************
	Create table 'category' in schema 'silver'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'category' AND schema_id = SCHEMA_ID('silver'))
	BEGIN
		PRINT ''
		PRINT '>> Creating category...';

		CREATE TABLE silver.category
		(
			cat_key VARCHAR(15),
			cat NVARCHAR(50),
			subcat NVARCHAR(50),
			maintenance VARCHAR(15),
			modified_date DATETIME2(0),
			row_hash BINARY(32)
		);
		CREATE INDEX nrix_category_cat_key ON silver.category(cat_key)	-- Join index. nonclustered since natural key is a varchar

		PRINT '>> Table category created successfully <<';
	END
	ELSE
		PRINT '>> Table category already exists';

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