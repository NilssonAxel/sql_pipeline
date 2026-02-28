/*###############################################################################################
Script:        DataWarehousev1 – Initial Database and Schema Setup
Author:        Axel Nilsson
Description:   This script creates a database and necessary schemas.
               Only in case no database named 'DataWarehousev1 exists.

Input:

   * None

Output:

   - Creates the following:

       * Database___DataWarehousev1
       * Schema_____landing
       * Schema_____bronze
       * Schema_____silver
       * Schema_____gold
       * Schema_____dq

Dependencies:

   * None

Notes:

   * Idempotent
   * Variables have been moved to below creating the database, due to GO-statement

###############################################################################################*/

/********************************************************
Initializing script
********************************************************/

USE master;
GO

PRINT '**********************************************************'
PRINT 'Switching database. Current database in use: ' + CAST(DB_NAME() AS VARCHAR(50))
PRINT '**********************************************************'
PRINT ''

PRINT '==========================================';
PRINT 'Initializing database: DataWarehousev1';
PRINT '==========================================';

/********************************************************
Create and switch to database 'DataWarehousev1'
********************************************************/

IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehousev1')
BEGIN
	PRINT '>> Creating database: DataWarehousev1...';

    CREATE DATABASE DataWarehousev1;
    	
    PRINT '>> Database DataWarehousev1 created successfully <<';
END
ELSE
    PRINT '>> Database DataWarehousev1 already exists';
GO

/********************************************************
Starting time counter for script
********************************************************/

DECLARE @script_time_started DATETIME2, @script_time_ended DATETIME2;

SET @script_time_started = SYSDATETIME();

BEGIN TRY    
    USE DataWarehousev1;
    PRINT ''
    PRINT '**********************************************************'
    PRINT 'Switching database. Current database in use: ' + CAST(DB_NAME() AS VARCHAR(50))
    PRINT '**********************************************************'
   
    /********************************************************
    Create Schema 'landing' in database 'DataWarehousev1'
    ********************************************************/
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'landing')
    BEGIN
        PRINT '';
		PRINT '>> Creating schema: landing...';

        EXEC('CREATE SCHEMA landing');
        
	    PRINT '>> Schema landing created successfully <<';
	END
	ELSE
	    PRINT '>> Schema landing already exists';

    /********************************************************
    Create Schema 'bronze' in database 'DataWarehousev1'
    ********************************************************/
      
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze')
    BEGIN
		PRINT '';
		PRINT '>> Creating schema: bronze...';

        EXEC('CREATE SCHEMA bronze');
                	
	    PRINT '>> Schema bronze created successfully <<';
	END
	ELSE
	    PRINT '>> Schema bronze already exists'; 
        
    /********************************************************
    Create Schema 'silver' in database 'DataWarehousev1'
    ********************************************************/
    
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver')
    BEGIN
		PRINT '';
		PRINT '>> Creating schema: silver...';

    EXEC('CREATE SCHEMA silver');
                	
	    PRINT '>> Schema silver created successfully <<';
	END
	ELSE
	    PRINT '>> Schema silver already exists'; 
        
    /********************************************************
    Create Schema 'gold' in database 'DataWarehousev1'
    ********************************************************/
    
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    BEGIN
		PRINT '';
		PRINT '>> Creating schema: gold...';

        EXEC('CREATE SCHEMA gold');

                	
	    PRINT '>> Schema gold created successfully <<';
	END
	ELSE
	    PRINT '>> Schema gold already exists';  
        
    /********************************************************
    Create Schema 'etl' in database 'DataWarehouse'
    ********************************************************/
    
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
    BEGIN
		PRINT '';
		PRINT '>> Creating schema: etl...';

        EXEC('CREATE SCHEMA etl');

                	
	    PRINT '>> Schema etl created successfully <<';
	END
	ELSE
	    PRINT '>> Schema etl already exists';  
	
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
	PRINT 'Error occured during initialization of database: DataWarehousev1';
	PRINT 'Error Message: ' + ERROR_MESSAGE();
	PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'Error State: ' + CAST (ERROR_STATE() AS NVARCHAR);
	PRINT 'Error Line: ' + CAST (ERROR_LINE() AS NVARCHAR);
	PRINT 'Error Severity: ' + CAST (ERROR_SEVERITY() AS NVARCHAR);
	PRINT 'Error Procedure: ' + CAST (ERROR_PROCEDURE() AS NVARCHAR);
	PRINT '================================================================';
END CATCH