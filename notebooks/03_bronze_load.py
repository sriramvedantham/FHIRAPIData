# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 03_bronze_load - Raw JSON -> Bronze Delta
# MAGIC Incrementally picks up raw page files not yet loaded (by `file_saved_at` watermark from `ops_api_call_log`), explodes each Bundle into one row per FHIR resource and appends to `bronze_<resource>`.
# MAGIC
# MAGIC Each row keeps the full resource JSON plus metadata: `extraction_timestamp` (API call time), `api_url_or_params`, `file_saved_at`, `raw_file_path`, `load_date`, `ingest_run_id`, `bronze_ingested_at`. Re-runs are idempotent (files being (re)loaded are deleted first).

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Find pending raw files
dbutils.widgets.text("resource", "Patient")
RESOURCE = validate_resource(dbutils.widgets.get("resource").strip())
TARGET = bronze_table(RESOURCE)
started_at = dt.datetime.utcnow()

wm = get_watermark("bronze", RESOURCE)
pending = spark.sql(f"""
  SELECT run_id, load_date, page_number, api_url_or_params, api_called_at, file_saved_at, raw_file_path
  FROM {T_API_LOG}
  WHERE resource_type = '{RESOURCE}' AND status = 'SUCCESS' AND raw_file_path IS NOT NULL
  {f"AND file_saved_at > TIMESTAMP'{wm}'" if wm else ""}""")
paths = [r.raw_file_path for r in pending.select("raw_file_path").collect()]
print(f"{RESOURCE}: {len(paths)} pending raw files (watermark={wm})")
if not paths:
    log_run("bronze", RESOURCE, started_at, "SUCCESS", 0, {"note": "no new raw files"})
    dbutils.notebook.exit(json.dumps({"resource": RESOURCE, "rows": 0}))

# COMMAND ----------

# DBTITLE 1,Explode bundles into resource rows
# binaryFile reads each page file as one record (pretty-printed multi-line JSON stays intact)
raw_pages = (spark.read.format("binaryFile").load(paths)
             .select(F.col("content").cast("string").alias("body"),
                     F.regexp_replace(F.col("path"), r"^(dbfs:|file:)", "").alias("raw_file_path")))
raw_pages.createOrReplaceTempView("raw_pages")
pending.createOrReplaceTempView("pending_files")

bronze_df = spark.sql(f"""
  SELECT
    e.value:resource:resourceType::string                                   AS resource_type,
    e.value:resource:id::string                                             AS resource_id,
    e.value:resource:meta.versionId::string                                 AS version_id,
    try_cast(e.value:resource:meta.lastUpdated::string AS TIMESTAMP)        AS last_updated,
    e.value:fullUrl::string                                                 AS full_url,
    to_json(e.value:resource)                                               AS resource_json,
    x.load_date, x.page_number,
    x.run_id                                                                AS ingest_run_id,
    x.api_url_or_params,
    x.api_called_at                                                         AS extraction_timestamp,
    x.file_saved_at,
    x.raw_file_path,
    current_timestamp()                                                     AS bronze_ingested_at,
    '{RUN_ID}'                                                              AS bronze_run_id
  FROM (SELECT p.raw_file_path, parse_json(p.body) AS bundle, f.* EXCEPT (raw_file_path)
        FROM raw_pages p JOIN pending_files f ON p.raw_file_path = f.raw_file_path) x,
       LATERAL variant_explode(x.bundle:entry) e
  WHERE e.value:resource:resourceType::string = '{RESOURCE}'""")

# COMMAND ----------

# DBTITLE 1,Write Bronze (idempotent) and advance watermark
if spark.catalog.tableExists(TARGET):
    spark.sql(f"DELETE FROM {TARGET} WHERE raw_file_path IN (SELECT raw_file_path FROM pending_files)")
    bronze_df.write.mode("append").saveAsTable(TARGET)
else:
    bronze_df.write.partitionBy("load_date").saveAsTable(TARGET)
    spark.sql(f"COMMENT ON TABLE {TARGET} IS 'Bronze: one row per FHIR {RESOURCE} resource per API page, full JSON + ingestion metadata (append-only)'")

loaded = spark.sql(f"SELECT count(*) c FROM {TARGET} WHERE raw_file_path IN (SELECT raw_file_path FROM pending_files)").first().c
new_wm = pending.agg(F.max("file_saved_at")).first()[0]
set_watermark("bronze", RESOURCE, str(new_wm))
log_run("bronze", RESOURCE, started_at, "SUCCESS", loaded, {"files": len(paths), "watermark": new_wm})
print(f"{RESOURCE}: loaded {loaded} rows from {len(paths)} files into {TARGET}")
dbutils.notebook.exit(json.dumps({"resource": RESOURCE, "rows": loaded, "files": len(paths)}))

# COMMAND ----------

