# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 06_validation_checks - post-load data quality and SCD2 integrity
# MAGIC Fails the job if any check fails:
# MAGIC * every successful API page landed in Bronze (entries = Bronze rows)
# MAGIC * Raw buckets do not overlap: each resource row's `last_updated` falls inside its `load_date` (UTC)
# MAGIC * exactly one `is_current` row per business key in Silver
# MAGIC * closed SCD2 rows have `effective_to >= effective_from`; current rows have `effective_to IS NULL`
# MAGIC * every Bronze id is present in Silver

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Run checks
started_at = dt.datetime.utcnow()
results = []

def check(name, resource, sql_value, expected=0):
    v = spark.sql(sql_value).first()[0]
    results.append((resource, name, int(v or 0), expected, int(v or 0) == expected))

for res, cfg in RESOURCES.items():
    B, S, K = bronze_table(res), silver_table(res), cfg["key"]
    check("api_entries_minus_bronze_rows", res, f"""
        SELECT (SELECT coalesce(sum(entry_count),0) FROM {T_API_LOG} WHERE resource_type='{res}' AND status='SUCCESS')
             - (SELECT count(*) FROM {B})""")
    check("rows_outside_load_date_bucket", res, f"SELECT count_if(to_date(last_updated) <> load_date) FROM {B}")
    check("keys_with_multiple_current_rows", res, f"SELECT count(*) FROM (SELECT {K} FROM {S} WHERE is_current GROUP BY {K} HAVING count(*) > 1)")
    check("invalid_scd2_intervals", res, f"""SELECT count_if((is_current AND effective_to IS NOT NULL) OR
                                                         (NOT is_current AND (effective_to IS NULL OR effective_to < effective_from))) FROM {S}""")
    check("bronze_ids_missing_in_silver", res, f"SELECT count(DISTINCT resource_id) FROM {B} b LEFT ANTI JOIN {S} s ON s.{K} = b.resource_id")

df = spark.createDataFrame(results, "resource_type string, check_name string, observed long, expected long, passed boolean")
display(df)
failed = [r for r in results if not r[4]]
log_run("validation", "ALL", started_at, "FAILED" if failed else "SUCCESS", len(results), {"failed": failed})
if failed:
    raise AssertionError(f"{len(failed)} validation checks failed: {failed}")

# COMMAND ----------

