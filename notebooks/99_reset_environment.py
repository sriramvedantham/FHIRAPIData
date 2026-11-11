# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 99_reset_environment - DESTRUCTIVE: wipe project data for a fresh full load
# MAGIC Drops Bronze/Silver tables, ops tables and Gold views in the project schema and deletes Raw files. Not part of the Job. Runs only when `confirm_reset = YES`.

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Reset
dbutils.widgets.dropdown("confirm_reset", "NO", ["NO", "YES"])
if dbutils.widgets.get("confirm_reset") != "YES":
    dbutils.notebook.exit("Skipped: set confirm_reset = YES to wipe the project data")

for v in [r.viewName for r in spark.sql(f"SHOW VIEWS IN {CATALOG}.{SCHEMA}").collect() if r.viewName.startswith("gold_")]:
    spark.sql(f"DROP VIEW IF EXISTS {tbl(v)}")
for res in RESOURCES:
    spark.sql(f"DROP TABLE IF EXISTS {bronze_table(res)}")
    spark.sql(f"DROP TABLE IF EXISTS {silver_table(res)}")
    dbutils.fs.rm(f"{RAW_ROOT}/{res.lower()}", True)
for t in [T_API_LOG, T_RUN_LOG, T_WATERMARK]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
display(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

