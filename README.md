# sql_datapipeline

A small SQL Server data pipeline built as a learning project and reusable base for future integrations with tools like dbt, Airflow, and similar frameworks.

The pipeline ingests fictional CRM and ERP data from CSV files, transforms and validates it through a layered architecture, and exposes clean, analytics-ready data in a gold layer.

---

## Architecture

The pipeline follows a medallion architecture with four layers:

**Landing** — Raw CSV files are bulk inserted as-is with no transformation. Acts as a staging area directly mirroring the source files.

**Bronze** — Append-only historical store. Every load is stamped with a `load_date`, preserving full history from both source systems.

**Silver** — Transformed, validated, and deduplicated data. Rows are flagged for data quality issues rather than deleted, giving full visibility into what was rejected.

**Gold** — Analytics-ready views built on top of silver. Only valid rows are surfaced. Includes a physical `dim_date` table for date dimension joins.

---

## Project Structure

```
sql_pipeline/
├── datasets/
│   ├── source_crm/
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/
│       ├── CUST_AZ12.csv
│       ├── LOC_A101.csv
│       └── PX_CAT_G1V2.csv
├── sql_scripts/
│   ├── 01_db/
│   │   └── init_db.sql              # Creates the database
│   ├── 02_etl/
│   │   └── init_etl.sql             # Sets up ETL logging and config
│   ├── 03_landing/
│   │   ├── init_landing.sql         # Creates landing tables
│   │   └── load_landing.sql         # Bulk inserts CSV data
│   ├── 04_bronze/
│   │   ├── init_bronze.sql          # Creates bronze tables
│   │   └── load_bronze.sql          # Loads and stamps data from landing
│   ├── 05_silver/
│   │   ├── inc_load/
│   │   │   ├── inc_load_silver_category.sql
│   │   │   ├── inc_load_silver_customer.sql
│   │   │   ├── inc_load_silver_product.sql
│   │   │   └── inc_load_silver_sales.sql
│   │   ├── init_load/
│   │   │   ├── init_load_silver_category.sql
│   │   │   ├── init_load_silver_customer.sql
│   │   │   ├── init_load_silver_product.sql
│   │   │   └── init_load_silver_sales.sql
│   │   └── init_silver.sql          # Creates silver tables and indexes
│   └── 06_gold/
│       ├── init_dimdate.sql         # Generates dim_date physical table
│       └── init_gold_views.sql      # Creates gold views
├── 01_setup.ps1                     # Creates database and ETL config
├── 02_init_sp.ps1                   # Sets up stored procedures
├── 03_init_load.ps1                 # Runs initial full load
└── 04_inc_load.ps1                  # Runs incremental load
```

---

## Prerequisites

- SQL Server (2016 or later)
- PowerShell (Or run everyhting manually in SQL Server)
- CSV dataset files placed in `datasets/source_crm/` and `datasets/source_erp/`

---

## Getting Started

Run the scripts in order from the repo root in PowerShell. All scripts default to `localhost` but accept a `-ServerInstance` parameter if your instance name differs.

**Step 1 — Set up the database structure**

Creates the database, schemas, tables, indexes, and gold views:

```powershell
.\01_setup.ps1
.\01_setup.ps1 -ServerInstance "MY-PC\SQLEXPRESS"
```

**Step 2 — Initialize stored procedures**

Creates the stored procedures used for loading data:

```powershell
.\02_init_sp.ps1
.\02_init_sp.ps1 -ServerInstance "MY-PC\SQLEXPRESS"
```

**Step 3 — Run the initial load**

Loads CSV data through landing → bronze → silver. Dataset paths default to the `datasets/` folder in the repo root but can be overridden:

```powershell
.\03_init_load.ps1
.\03_init_load.ps1 -ServerInstance "MY-PC\SQLEXPRESS" -dataset_path_crm "C:\your\crm\path\" -dataset_path_erp "C:\your\erp\path\"
```

**Step 4 — Run incremental loads**

For subsequent loads after new data has been added to the CSV files:

```powershell
.\04_inc_load.ps1
.\04_inc_load.ps1 -ServerInstance "MY-PC\SQLEXPRESS" -dataset_path_crm "C:\your\crm\path\" -dataset_path_erp "C:\your\erp\path\"
```

---

## Design Decisions

**Incremental loads over full reloads** — Bronze keeps full history using `load_date` stamping. Silver uses merge with hash comparison to only update rows that have genuinely changed.

**Status flagging over deletion** — Invalid rows in silver.sales are flagged with a status code rather than removed. This preserves auditability and makes it easy to diagnose data quality issues upstream.

**No surrogate keys in silver** — Natural keys from the source systems are used throughout silver. Surrogate keys are not necessary for SCD1 dimensions where each entity has exactly one current record.

**Gold as views** — Gold is intentionally kept as views over silver so there is no duplication of data and the logic stays in one place. The exception is `dim_date` which is a physical table since it is generated once and has no source system to derive from.

---

## Future Integrations

This project is designed as a foundation for:

- **dbt** — transformation layer on top of silver or gold
- **Airflow** — orchestration of the full pipeline
- **Power BI / reporting tools** — connecting directly to gold views
