"""
Mock Azure Data Pipeline: Oracle WMS ➝ Azure Blob ➝ PySpark (Databricks) ➝ Snowflake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# ---------------------------------------
# Step 1: Spark Session (Databricks)
# ---------------------------------------
spark = SparkSession.builder \
    .appName("WMS to Snowflake CDC Pipeline") \
    .getOrCreate()

# ---------------------------------------
# Step 2: Simulate loading incremental data from Blob Storage
# (Real: wasbs://container@account.blob.core.windows.net/...)
# ---------------------------------------
raw_df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("path/to/wms_blob_exports/*.csv")

# ---------------------------------------
# Step 3: Mock Change Data Capture (CDC) filter
# ---------------------------------------
cdc_df = raw_df.filter(col("last_updated") >= "2023-12-01") \
               .withColumn("ingestion_time", current_timestamp())

# ---------------------------------------
# Step 4: Transform for Snowflake schema
# ---------------------------------------
transformed_df = cdc_df.select(
    col("item_id").cast("string"),
    col("warehouse_location"),
    col("quantity").cast("int"),
    col("last_updated").cast("timestamp"),
    col("ingestion_time")
)

# ---------------------------------------
# Step 5: Write to Snowflake (mock)
# ---------------------------------------
transformed_df.write.format("snowflake") \
    .option("sfURL", "account.region.snowflakecomputing.com") \
    .option("sfDatabase", "WMS_DB") \
    .option("sfSchema", "STAGING") \
    .option("sfWarehouse", "COMPUTE_WH") \
    .option("sfRole", "SYSADMIN") \
    .option("sfUser", "<your_username>") \
    .option("sfPassword", "<your_password>") \
    .option("dbtable", "STG_WMS_INVENTORY") \
    .mode("append") \
    .save()

# ---------------------------------------
# Step 6: Optional CI/CD Integration (mock)
# ---------------------------------------
print("CDC pipeline executed. Ready for CI/CD integration via Azure DevOps.")
