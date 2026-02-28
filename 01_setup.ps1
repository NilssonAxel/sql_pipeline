<#
    This script initializes the database and sets up the necessary schemas, tables, and views for the data warehouse project.
    It should be run before any data loading scripts to ensure that the database structure is in place.
    The script uses the Invoke-Sqlcmd cmdlet to execute SQL scripts that create the database and its components.
    The default server instance is set to "localhost", but it can be overridden by providing a different value when running the script.
#>

param(
    [string]$ServerInstance = "localhost"   # Default value for the server instance, can be overridden when running the script
)

Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\01_db\init_db.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\02_etl\init_etl.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\03_landing\init_landing.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\04_bronze\init_bronze.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\05_silver\init_silver.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\06_gold\init_gold_views.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\06_gold\init_dimdate.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green