/*###############################################################################################
Script:        DataWarehouse – Dim_date setup for schema: gold
Author:        Axel Nilsson
Description:   This script creates and fills table dim_date for the schema gold.
               Only in case database named 'DataWarehouse' exists.

Input:

   * None

Output:

   - Creates the following:

	   * Table___gold.dim_date

	- Generates the following:
		
	   * Values___gold.dim_date

Dependencies:

   * Database: DataWarehouse
   * Schema: gold

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

DECLARE @update_dimdate BIT = 0;		-- Set to 1 if you wish to add more dates to the dim_date table
DECLARE @start_date DATE = '2010-01-01';
DECLARE @end_date DATE = '2015-01-01';
DECLARE @fiscal_start_month INT = 6;

DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

SET @script_time_started = SYSDATETIME();

BEGIN TRY
BEGIN TRAN
	/********************************************************
	Create table 'dim_date' in schema 'gold'
	********************************************************/

	IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dim_date' AND schema_id = SCHEMA_ID('gold'))
	BEGIN
		PRINT '';
		PRINT '>> Creating dim_date...';

		CREATE TABLE gold.dim_date
		(
			DateKey INT PRIMARY KEY,
			DateValue DATE,
			FullDate NVARCHAR(20),
			DayofWeekName NVARCHAR(10),
			DayofWeekShort NVARCHAR(3),
			DayofWeekNumber TINYINT,
			DayNumberInMonth TINYINT,
			DayNumberInYear SMALLINT,
			[Week] TINYINT,
			[MonthName] NVARCHAR(9),
			MonthNameShort NVARCHAR(4),
			MonthNumber TINYINT,
			YearMonth NVARCHAR(15),
			QuarterName NVARCHAR(2),
			QuarterNumber TINYINT,
			YearQuarterName NVARCHAR(7),
			YearQuarterNumber NVARCHAR(6),
			YearValue SMALLINT,
			WeekdayFlag BIT,
			FiscalYear INT
		);

		PRINT '>> Table dim_date created successfully <<';
	END
	ELSE
		PRINT '>> Table dim_date already exists';

	/********************************************************
	Generating values for table 'dim_date' in schema 'gold'
	********************************************************/
	-- Using a while loop instead of the tally method for readability

	IF NOT EXISTS (SELECT 1 FROM gold.dim_date) OR @update_dimdate = 1
	BEGIN

	DECLARE @date DATE = @start_date

	PRINT '>> Generating values in dim_date...';

		WHILE @date <= @end_date
		BEGIN
			IF @date NOT IN (SELECT FullDate FROM gold.dim_date)
			BEGIN
				INSERT INTO gold.dim_date
					(
					DateKey,
					DateValue,
					FullDate,
					DayofWeekName,
					DayofWeekShort,
					DayofWeekNumber,
					DayNumberInMonth,
					DayNumberInYear,
					[Week],
					[MonthName],
					MonthNameShort,
					MonthNumber,
					YearMonth,
					QuarterName,
					QuarterNumber,
					YearQuarterName,
					YearQuarterNumber,
					YearValue,
					WeekdayFlag,
					FiscalYear
					)
				VALUES
					(
					CONVERT(INT, CONVERT(CHAR(8), @date, 112)),								--DateKey
					@date,																	--DateValue
					FORMAT(@date, 'dd MMMM yyyy', 'en-US'),									--FullDate
					DATENAME(WEEKDAY, @date),												--DayofWeekName
					LEFT(DATENAME(WEEKDAY, @date), 3),										--DayofWeekShort
					DATEPART(WEEKDAY, @date),												--DayofWeekNumber
					DATEPART(DAY, @date),													--DayNumberInMonth
					DATEPART(DAYOFYEAR, @date),												--DayNumberInYear
					DATEPART(WEEK, @date),													--Week
					DATENAME(MONTH, @date),													--MonthName
					LEFT(DATENAME(MONTH, @date), 3),										--MonthNameShort
					DATEPART(MONTH, @date),													--MonthNumber
					CONCAT(DATEPART(YEAR, @date), '-', DATEPART(MONTH, @date)),				--YearMonth
					CONCAT('Q', DATEPART(QUARTER, @date)),									--QuarterName
					DATEPART(QUARTER, @date),												--QuarterNumber
					CONCAT(DATEPART(YEAR, @date), '-Q', DATEPART(QUARTER, @date)),			--YearQuarterName 
					CONCAT(DATEPART(YEAR, @date), '-', DATEPART(QUARTER, @date)),			--YearQuarterNumber
					DATEPART(YEAR, @date),													--YearValue
					CASE WHEN DATEPART(WEEKDAY, @date) BETWEEN 1 AND 5 THEN 1 ELSE 0 END,	--WeekdayFlag
					DATEPART(YEAR, DATEADD(MONTH, -@fiscal_start_month + 1, @date))			--FiscalYear
					);
				END
			SET @date = DATEADD(DAY, 1, @date)
		END

		PRINT '>> Values generated in dim_date <<';
	END
	ELSE
		PRINT '>> Values already exists'

	/********************************************************
	Finishing script
	********************************************************/
	
	SET NOCOUNT OFF;
	
	COMMIT TRAN;

	SET @script_time_ended = SYSDATETIME();

	PRINT '';
	PRINT '-------------------------------------------';
	PRINT 'The script ended successfully';
	PRINT 'Loading time was: ' + CAST(DATEDIFF(SECOND, @script_time_started, @script_time_ended) AS VARCHAR(25)) + ' seconds';
END TRY
BEGIN CATCH
	ROLLBACK TRAN;
	PRINT '================================================================';
	PRINT 'Error occured during creation of tables for schema: gold';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
	PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
	PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
	PRINT 'No data was inserted to dim_date'
	PRINT '================================================================';
END CATCH