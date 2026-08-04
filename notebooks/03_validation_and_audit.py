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