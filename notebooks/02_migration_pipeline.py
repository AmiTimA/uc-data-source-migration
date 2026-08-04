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