# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 04_silver_scd2 - Bronze -> Silver (clean, dedupe, SCD Type 2)
# MAGIC Generic for every resource; the column mapping comes from `RESOURCES` in `00_config`.
# MAGIC
# MAGIC 1. Read Bronze rows newer than the `silver` watermark (`bronze_ingested_at`).
# MAGIC 2. Flatten FHIR JSON to typed columns, trim/standardise values, drop rows without a business key.
# MAGIC 3. Deduplicate: one row per (`key`, `last_updated`) - overlapping/re-fetched pages collapse to one.
# MAGIC 4. `record_hash` = SHA-256 of the business columns (FHIR `meta` excluded), used for change detection.
# MAGIC 5. SCD2 MERGE, applied in version order so multiple versions in one batch are all kept:
# MAGIC    * new key -> insert current row
# MAGIC    * changed hash -> close current row (`effective_to`, `is_current=false`) and insert the new version
# MAGIC    * same hash or older-than-current record -> ignored

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Parameters and incremental source
dbutils.widgets.text("resource", "Patient")
RESOURCE = validate_resource(dbutils.widgets.get("resource").strip())
cfg = RESOURCES[RESOURCE]
KEY = cfg["key"]
SRC, TGT = bronze_table(RESOURCE), silver_table(RESOURCE)
started_at = dt.datetime.utcnow()

wm = get_watermark("silver", RESOURCE)
src = spark.table(SRC)
if wm:
    src = src.where(F.col("bronze_ingested_at") > F.lit(wm).cast("timestamp"))
src_count = src.count()
print(f"{RESOURCE}: {src_count} new bronze rows (watermark={wm})")
if src_count == 0:
    log_run("silver", RESOURCE, started_at, "SUCCESS", 0, {"note": "no new bronze rows"})
    dbutils.notebook.exit(json.dumps({"resource": RESOURCE, "rows": 0}))
new_wm = src.agg(F.max("bronze_ingested_at")).first()[0]

# COMMAND ----------

# DBTITLE 1,Flatten, clean, hash, dedupe
biz_cols = list(cfg["columns"])
select_sql = ",\n    ".join(f"{expr} AS {name}" for name, expr in {**cfg["columns"], **META_COLUMNS}.items())
hash_sql = "sha2(to_json(named_struct(" + ", ".join(f"'{c}', {c}" for c in biz_cols) + ")), 256)"

src.createOrReplaceTempView("bronze_increment")
staged = spark.sql(f"""
WITH parsed AS (
  SELECT
    {select_sql},
    extraction_timestamp, api_url_or_params, raw_file_path, bronze_ingested_at
  FROM (SELECT *, parse_json(resource_json) AS r FROM bronze_increment)
),
clean AS (
  SELECT *, {hash_sql} AS record_hash,
         coalesce(last_updated, extraction_timestamp) AS effective_from
  FROM parsed WHERE {KEY} IS NOT NULL AND trim({KEY}) <> ''
),
dedup AS (
  SELECT * FROM clean
  QUALIFY row_number() OVER (PARTITION BY {KEY}, effective_from ORDER BY extraction_timestamp DESC, bronze_ingested_at DESC) = 1
)
SELECT *, row_number() OVER (PARTITION BY {KEY} ORDER BY effective_from) AS version_seq FROM dedup""")
staged.createOrReplaceTempView("silver_staged")

stats = spark.sql("SELECT count(*) n, count(DISTINCT " + KEY + ") k, max(version_seq) m FROM silver_staged").first()
print(f"staged rows={stats.n}, distinct keys={stats.k}, max versions in batch={stats.m}, dropped(no key or dup)={src_count - stats.n}")

# COMMAND ----------

# DBTITLE 1,Create Silver table if needed
SCD_COLS = """effective_from, CAST(NULL AS TIMESTAMP) AS effective_to, true AS is_current,
              current_timestamp() AS silver_loaded_at, current_timestamp() AS silver_updated_at, '{run}' AS silver_run_id""".format(run=RUN_ID)
data_cols = biz_cols + list(META_COLUMNS) + ["record_hash", "extraction_timestamp", "api_url_or_params", "raw_file_path"]

if not spark.catalog.tableExists(TGT):
    (spark.sql(f"SELECT {', '.join(data_cols)}, {SCD_COLS} FROM silver_staged LIMIT 0")
          .write.saveAsTable(TGT))
    spark.sql(f"COMMENT ON TABLE {TGT} IS 'Silver: cleaned {RESOURCE} with SCD Type 2 history (business key {KEY}; is_current marks the active version)'")

target_cols = [c for c in spark.table(TGT).columns]
print(TGT, target_cols)

# COMMAND ----------

# DBTITLE 1,SCD Type 2 merge (in version order)
insert_cols = ", ".join(target_cols)
insert_vals = ", ".join(f"s.{c}" for c in target_cols)
totals = {"inserted": 0, "closed": 0}

for seq in range(1, int(stats.m or 0) + 1):
    # Only rows that are new keys, or differ from the current version and are not older than it
    spark.sql(f"""
      CREATE OR REPLACE TEMP VIEW scd_batch AS
      SELECT s.{', s.'.join(data_cols)}, {SCD_COLS.replace('effective_from,', 's.effective_from AS effective_from,', 1)}
      FROM silver_staged s
      LEFT JOIN {TGT} c ON c.{KEY} = s.{KEY} AND c.is_current
      WHERE s.version_seq = {seq}
        AND (c.{KEY} IS NULL OR (c.record_hash <> s.record_hash AND s.effective_from >= c.effective_from))""")

    res = spark.sql(f"""
      MERGE INTO {TGT} t
      USING (
        SELECT b.{KEY} AS merge_key, b.* FROM scd_batch b
        UNION ALL
        SELECT NULL AS merge_key, b.* FROM scd_batch b
        JOIN {TGT} c ON c.{KEY} = b.{KEY} AND c.is_current
      ) s
      ON t.{KEY} = s.merge_key AND t.is_current
      WHEN MATCHED AND t.record_hash <> s.record_hash THEN UPDATE SET
        t.is_current = false, t.effective_to = s.effective_from, t.silver_updated_at = current_timestamp()
      WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})""").first().asDict()
    totals["inserted"] += res.get("num_inserted_rows", 0)
    totals["closed"]   += res.get("num_updated_rows", 0)
    print(f"version_seq {seq}: {res}")

set_watermark("silver", RESOURCE, str(new_wm))
log_run("silver", RESOURCE, started_at, "SUCCESS", totals["inserted"],
        {"bronze_rows": src_count, "staged_rows": stats.n, **totals, "watermark": new_wm})
dbutils.notebook.exit(json.dumps({"resource": RESOURCE, **totals}))

# COMMAND ----------

