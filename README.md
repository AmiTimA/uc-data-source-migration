# Databricks Data Migration: Legacy Hive Metastore to Unity Catalog

## Project Overview
This project demonstrates an end-to-end data migration pipeline moving legacy unmanaged CSV tables from the **Hive Metastore (DBFS)** to managed Delta Lake tables within **Databricks Unity Catalog**.

## Architecture & Migration Strategy

[Legacy State] [Modern State]
Hive Metastore Unity Catalog
hive_metastore.legacy_db.legacy_orders main.migrated_sales.orders_fact
(CSV Format in DBFS) ====> (Managed Delta Lake Table)
Legacy Access Controls Centralized Grants / Lineage

### Key Migration Objectives:
1. **Metadata & Governance Migration:** Move from 2-level namespace (`schema.table`) to 3-level namespace (`catalog.schema.table`).
2. **Format Modernization:** Convert raw CSV files to ACID-compliant Delta Lake format.
3. **Data Quality & Validation:** Enforce schema definitions, type casting, and audit log tracking (`_ingestion_timestamp`).
4. **Data Reconciliation:** Automated row-count and sum-checksum verification to guarantee data parity.

## Repository Structure
* `notebooks/01_setup_legacy_environment.py`: Seeds fake legacy CSV data into Hive Metastore.
* `notebooks/02_migration_pipeline.py`: PySpark ETL pipeline executing the migration and transformation to UC.
* `notebooks/03_validation_and_audit.py`: Automated testing suite validating data completeness and integrity.

## How to Run
1. Import the notebooks into your Databricks Workspace.
2. Run `01_setup_legacy_environment.py` to create the mock source system.
3. Execute `02_migration_pipeline.py` to run the migration logic.
4. Run `03_validation_and_audit.py` to verify data consistency.