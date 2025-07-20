# Snowflake-pipeline
---

## 🔁 Mock Azure Data Pipeline (CDC Flow)

Simulates a real-world ETL pipeline that ingests Oracle WMS data exported to Azure Blob Storage, applies a mock Change Data Capture (CDC) filter via PySpark (Databricks), and loads the transformed results into Snowflake.

### 💡 Flow

**Oracle WMS ➝ Azure Blob ➝ PySpark (Databricks) ➝ Snowflake**

### 🧱 Steps

1. **Spark Session** created in Databricks
2. **Blob ingestion** from CSV exports
3. **CDC logic**: filters rows based on `last_updated` column
4. **Transformation**: type casting and schema enforcement
5. **Snowflake Write** using `spark-snowflake-connector`
6. **CI/CD Ready** for Azure DevOps or GitHub Actions

### 📄 File
See: [`wms_to_snowflake_pipeline.py`](backend/pipelines/wms_to_snowflake_pipeline.py)

---

## 🧰 Requirements

Make sure these are in your `requirements.txt`:

```txt
pyspark
snowflake-spark-connector
