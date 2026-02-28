<#
    This script initializes the stored procedures for loading data into the landing, bronze, and silver layers of the data warehouse.
    It should be run after the database has been set up using the 01_setup.ps1 script, altough it's not a requirement.
    The script uses the Invoke-Sqlcmd cmdlet to execute SQL scripts that load data into the respective layers of the data warehouse.
    The default server instance is set to "localhost", but it can be overridden by providing a different value when running the script.
#>

param(
    [string]$ServerInstance = "localhost"   # Default value for the server instance, can be overridden when running the script
)


Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\03_landing\load_landing.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\04_bronze\load_bronze.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\05_silver\inc_load\inc_load_silver_customer.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\05_silver\inc_load\inc_load_silver_product.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\05_silver\inc_load\inc_load_silver_category.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -InputFile ".\sql_scripts\05_silver\inc_load\inc_load_silver_sales.sql" -Verbose
Write-Host "################################################################################" -ForegroundColor Green