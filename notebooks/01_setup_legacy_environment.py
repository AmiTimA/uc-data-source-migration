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