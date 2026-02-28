/*###############################################################################################
Script:        DataWarehouse – View setup for schema: gold
Author:        Axel Nilsson
Description:   This script creates views for the schema gold.
               Only in case database named 'DataWarehouse' exists.

Input:

   * None

Output:

   - Creates the following:

       * View___gold.dim_customer
       * View___gold.dim_product
	   * View___gold.fact_sales


Dependencies:

   * Database: DataWarehouse
   * Schema: gold
   * Table: silver.customer
   * Table: silver.product
   * Table: silver.category
   * Table: silver.sales

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

/********************************************************
Create Views
********************************************************/

PRINT '==========================================';
PRINT 'Creating views for schema: gold';
PRINT '==========================================';
GO

/********************************************************
Create table 'dim_customer' in schema 'gold'
********************************************************/
CREATE OR ALTER VIEW gold.dim_customer AS
	SELECT
		cst_id				AS customer_id,
		cst_key				AS customer_number,
		cst_firstname		AS customer_firstname,
		cst_lastname		AS customer_lastname,
		cst_marital_status	AS customer_marital_status,
		cst_gender			AS customer_gender,
		cst_country			AS customer_country,
		cst_birthdate		AS customer_birthdate,
		cst_create_date		AS customer_create_date
	FROM silver.customer
GO

/********************************************************
Create table 'dim_product' in schema 'gold'
********************************************************/
CREATE OR ALTER VIEW gold.dim_product AS
	SELECT
		p.prd_id		AS product_id,
		p.prd_key		AS product_number,
		p.prd_name		AS product_name,
		p.prd_cost		AS product_cost,
		p.prd_line		AS product_line,
		c.cat_key		AS category_id,
		c.cat			AS category_name,
		c.subcat		AS subcategory_name,
		c.maintenance	AS maintenance,
		p.valid_from	AS valid_from
	FROM silver.product p
	JOIN silver.category c ON p.cat_key = c.cat_key
GO

/********************************************************
Create table 'fact_sales' in schema 'gold'
********************************************************/
CREATE OR ALTER VIEW gold.fact_sales AS
	SELECT
		sls_ord_num AS sales_order_number,
		sls_prd_key AS sales_product_number,
		sls_cust_id AS sales_customer_id,
		sls_order_dt AS sales_order_date,
		sls_ship_dt AS sales_ship_date,
		sls_due_dt AS sales_due_date,
		sls_sales AS total_sales,
		sls_quantity AS sales_quantity,
		sls_price AS sales_price
	FROM silver.sales
	WHERE status = 1
GO

PRINT '>> Views created successfully <<';