# uc-data-source-migration
## Task: uc-data-source-migration-legacy-to-modern

## Databricks

Databricks is a cloud-based data and AI platform built on Apache Spark that unifies data lakes and data warehouses into a single "lakehouse" architecture. It provides scalable data processing, collaborative notebooks, SQL analytics, and machine learning tooling on top of open storage formats such as Delta Lake, and runs on AWS, Azure, and Google Cloud.

## Unity Catalog

Unity Catalog (UC) is Databricks' unified governance layer for data and AI assets. It provides centralized access control, auditing, data lineage, and discovery across all workspaces in a Databricks account. Assets are organized using a three-level namespace (`catalog.schema.table`), and UC governs tables, views, volumes, ML models, and functions through a single permission model backed by open standards.

# Important Note on Databricks Free Tier
- Databricks Community Edition (CE) does NOT support Unity Catalog (it only has Hive Metastore).
- Databricks 14-day Free Trial (on AWS, Azure, or GCP) DOES support Unity Catalog.
- If you are on Community Edition: You can still complete this project! You will write code that explicitly handles `hive_metastore` vs simulated UC catalogs, and document it clearly in your GitHub README.

# Project Overview & Architecture
## Legacy State (Before)
- Catalog System: Hive Metastore (hive_metastore.default.table_name)
- Storage Location: DBFS Root (dbfs:/user/hive/warehouse/) or legacy DBFS mounts (dbfs:/mnt/data/).
- Data Format: Unmanaged CSV/Parquet or legacy Delta tables.
- Access Control: Table ACLs (Workspaces-specific, hard to maintain).
## Modern State (After - Unity Catalog)
- Catalog System: Unity Catalog 3-level namespace (catalog.schema.table -> e.g., prod_catalog.finance.orders).
- Storage Location: Cloud Storage (AWS S3 / ADLS Gen2) backed by Storage Credentials & External Locations managed by UC.
- Data Format: Managed Delta Lake tables (with ACID transactions, Time Travel, Lineage).
- Access Control: Centralized UC Grants (GRANT SELECT ON TABLE...).

# Step 1: Set Up Your GitHub Repository
1. Create a public repository on GitHub named: uc-data-source-migration-legacy-to-modern
2. Local/Git folder structure to set up:
```
uc-data-source-migration-legacy-to-modern/
├── README.md
├── docs/
│   └── architecture_diagram.png
├── notebooks/
│   ├── 01_setup_legacy_environment.py
│   ├── 02_migration_pipeline.py
│   └── 03_validation_and_audit.py
└── sql/
    ├── legacy_teardown.sql
    └── uc_grants_setup.sql
```

# Step 2: Build the Code (Step-by-Step)

## Notebook 1: Setup Legacy Environment (01_setup_legacy_environment.py)

This script creates legacy datasets in Hive Metastore (simulating the legacy system you need to migrate).

```python
# Databricks notebook source
# DBTITLE 1, Create Dummy Legacy Data
from pyspark.sql.functions import col, current_timestamp, expr

# Create a sample DataFrame (Legacy E-commerce Data)
data = [
    (101, "Alice", "Laptop", 1200.50, "2023-01-15"),
    (102, "Bob", "Phone", 800.00, "2023-01-16"),
    (103, "Charlie", "Tablet", 450.00, "2023-01-17"),
    (104, "David", "Monitor", 300.00, "2023-01-18")
]

columns = ["order_id", "customer_name", "product", "amount", "order_date"]
df = spark.createDataFrame(data, columns)

# Write as Legacy CSV to DBFS Mount/Root
dbfs_legacy_path = "/tmp/legacy_data/orders_csv"
df.write.mode("overwrite").option("header", "true").csv(dbfs_legacy_path)

# Register as Unmanaged/Legacy Table in Hive Metastore
spark.sql("CREATE DATABASE IF NOT EXISTS hive_metastore.legacy_db")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS hive_metastore.legacy_db.legacy_orders (
        order_id INT,
        customer_name STRING,
        product STRING,
        amount DOUBLE,
        order_date STRING
    )
    USING CSV
    OPTIONS (path '{dbfs_legacy_path}', header 'true')
""")

print("Legacy data created successfully in Hive Metastore!")
```

## Notebook 2: The Migration Engine (02_migration_pipeline.py)

This is the core of your project. It converts legacy data formats to Delta and moves them into Unity Catalog.

```python
# Databricks notebook source
# DBTITLE 1, Define Parameters
TARGET_CATALOG = "main" # Replace with your UC Catalog (or 'hive_metastore' if using CE)
TARGET_SCHEMA = "migrated_sales"
TARGET_TABLE = "orders_fact"

# DBTITLE 2, Create Unity Catalog Target Schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {TARGET_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

# DBTITLE 3, Migrate & Modernize Data
# 1. Read from Legacy Source
df_legacy = spark.table("hive_metastore.legacy_db.legacy_orders")

# 2. Apply Modernizations (Type Casting, Adding Metadata)
df_modern = df_legacy \
    .withColumn("order_id", col("order_id").cast("integer")) \
    .withColumn("amount", col("amount").cast("decimal(10,2)")) \
    .withColumn("order_date", col("order_date").cast("date")) \
    .withColumn("_ingestion_timestamp", current_timestamp()) \
    .withColumn("_source_system", expr("'legacy_hive_metastore'"))

# 3. Write to Unity Catalog as Managed Delta Table
full_uc_table_path = f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{TARGET_TABLE}"

df_modern.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(full_uc_table_path)

print(f"Successfully migrated data to Modern UC Table: {full_uc_table_path}")
```

## Notebook 3: Data Reconciliation & Validation (03_validation_and_audit.py)

In real migrations, you must prove no data was lost or corrupted during the move.

```python
# Databricks notebook source
# DBTITLE 1, Audit & Data Integrity Checks
source_table = "hive_metastore.legacy_db.legacy_orders"
target_table = "main.migrated_sales.orders_fact" # Adjust catalog name if needed

# 1. Row Count Validation
source_count = spark.table(source_table).count()
target_count = spark.table(target_table).count()

print(f"Source Row Count: {source_count}")
print(f"Target Row Count: {target_count}")

assert source_count == target_count, "DATA LOSS DETECTED: Row counts do not match!"

# 2. Sum Checksum Validation
source_sum = spark.table(source_table).selectExpr("sum(cast(amount as double))").collect()[0][0]
target_sum = spark.table(target_table).selectExpr("sum(amount)").collect()[0][0]

print(f"Source Total Amount: {source_sum}")
print(f"Target Total Amount: {target_sum}")

assert source_sum == target_sum, "DATA CORRUPTION DETECTED: Total amounts do not match!"

print("MIGRATION VALIDATION SUCCESSFUL: Data parity confirmed.")
```

# Step 3: Check README.md file

# Step 4: Explaination

## What is this migration project?

1. Context: "A migration framework to move legacy datasets from the legacy Hive Metastore into Unity Catalog to leverage centralized governance, lineage, and Delta performance features."
2. Problem Addressed: "Legacy tables stored as raw CSVs in DBFS lack ACID guarantees, fine-grained access control, and schema enforcement."
3. Approach Taken:
    - Created a PySpark migration pipeline that reads legacy unmanaged tables.
    - Transformed, cast, and enriched the data with metadata tracking columns.
    - Target tables were written out as Managed Delta tables in Unity Catalog using the 3-level namespace (catalog.schema.table).
    - Built an automated reconciliation script executing count and hash/sum checks to ensure 100% data parity post-migration.
4. Enterprise Knowledge Bonus Points (Say this!):
    - Mention Databricks UCX: "For large-scale enterprise migrations, I am also familiar with using Databricks' official UCX (Unity Catalog Migration Assistant) CLI tool to automate table upgrades and permission mappings."
    - Mention SYNC command / Shallow Clones: "If migrating existing Delta tables, I would leverage CREATE TABLE ... DEEP CLONE or the SYNC command for seamless metadata upgrades without re-writing underlying data."

# Next Steps to Start Now:
1. Open GitHub → Create repo uc-data-source-migration-legacy-to-modern.
2. Open Databricks (Free Trial or Community Edition).
3. Copy-paste the 3 PySpark scripts into notebooks.
4. Run them in Databricks to make sure they run error-free.
5. Push your code and README to your GitHub repo.