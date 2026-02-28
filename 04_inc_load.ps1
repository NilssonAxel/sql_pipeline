 <#
    This script queries the stored procedures to load data into the landing, bronze, and incrementally into silver layers of the data warehouse.
    It should be run after the database has been set up using the 01_setup.ps1 script, and after the stored procedures have been initialized using the 02_init_sp.ps1 script.
    The script uses the Invoke-Sqlcmd cmdlet to execute SQL scripts that load data into the respective layers of the data warehouse.
    The default server instance is set to "localhost", but it can be overridden by providing a different value when running the script.
    The dataset paths for CRM and ERP data can also be overridden when running the script, allowing for flexibility in data sources, 
    although the default paths are set to specific directories within the project structure and should not be changed unless necessary.
 #>
 
 param(
    [string]$ServerInstance = "localhost",   # Default value for the server instance, can be overridden when running the script
    [string]$dataset_path_crm = "$PSScriptRoot\datasets\source_crm\",  # Default value for the dataset path, can be overridden when running the script
    [string]$dataset_path_erp = "$PSScriptRoot\datasets\source_erp\"  # Default value for the dataset path, can be overridden when running the script
)


Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -Database "DataWarehousev1" -Query "EXEC landing.load_landing @path_crm='$dataset_path_crm', @path_erp='$dataset_path_erp'" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -Database "DataWarehousev1" -Query "EXEC bronze.load_bronze" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -Database "DataWarehousev1" -Query "EXEC silver.load_silver_customer" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -Database "DataWarehousev1" -Query "EXEC silver.load_silver_product" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -Database "DataWarehousev1" -Query "EXEC silver.load_silver_category" -Verbose
Write-Host "################################################################################" -ForegroundColor Green
Invoke-Sqlcmd -ServerInstance $ServerInstance -Database "DataWarehousev1" -Query "EXEC silver.load_silver_sales" -Verbose
Write-Host "################################################################################" -ForegroundColor Green