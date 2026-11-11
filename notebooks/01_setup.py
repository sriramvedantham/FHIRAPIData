# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 01_setup - idempotent environment bootstrap
# MAGIC Creates the schema, the Raw landing volume, per-resource Raw folders, and the ops/metadata tables. Safe to re-run.

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Create schema, volume, raw folders
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'FHIR R4 medallion lakehouse (Raw/Bronze/Silver/Gold)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{RAW_VOLUME} COMMENT 'Raw layer: FHIR API responses stored as-is, bucketed by resource and load date'")

for res in RESOURCES:
    dbutils.fs.mkdirs(f"{RAW_ROOT}/{res.lower()}")
print(dbutils.fs.ls(RAW_ROOT))

# COMMAND ----------

# DBTITLE 1,Create ops / metadata tables
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_API_LOG} (
  log_id STRING, run_id STRING, resource_type STRING, load_date DATE, page_number INT,
  api_url_or_params STRING COMMENT 'Exact URL called (incl. query params / next-page link)',
  request_params STRING COMMENT 'JSON of logical request parameters',
  http_status INT, entry_count INT,
  api_called_at TIMESTAMP COMMENT 'When the API request was sent (UTC)',
  response_received_at TIMESTAMP,
  file_saved_at TIMESTAMP COMMENT 'When the raw response was written to the Raw volume',
  raw_file_path STRING, response_bytes LONG, attempts INT, status STRING, error_message STRING)
COMMENT 'One row per FHIR API call (page). Tracks when each API was called and when its data was saved.'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_RUN_LOG} (
  run_id STRING, layer STRING, resource_type STRING, started_at TIMESTAMP, finished_at TIMESTAMP,
  status STRING, rows_processed LONG, details STRING)
COMMENT 'One row per notebook/task execution across all medallion layers.'""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {T_WATERMARK} (
  layer STRING, resource_type STRING, watermark_value STRING, updated_at TIMESTAMP, run_id STRING)
COMMENT 'Incremental-load high-water marks. raw = last API load_date fetched; silver = last bronze_ingested_at processed.'""")

display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

