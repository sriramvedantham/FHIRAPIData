# FHIR API Data Ingestion & Analytics - Databricks Medallion Lakehouse

Incremental ingestion of the public HAPI FHIR R4 API (`https://hapi.fhir.org/baseR4`) for **Patient, Encounter, Observation, Condition** into a Raw -> Bronze -> Silver (SCD Type 2) -> Gold lakehouse on Databricks (Unity Catalog, Delta, serverless), orchestrated by a Lakeflow Job.

Full design, data dictionary, table relationships and run results: [docs/Solution_Document.md](docs/Solution_Document.md)

## 1. Architecture

```
HAPI FHIR API (JSON, paginated via Bundle.link[next])
        |  02_raw_ingest_api   (per resource, per UTC day bucket)
        v
RAW     /Volumes/<catalog>/<schema>/raw/<resource>/load_date=YYYY-MM-DD/run_id=<run>/page_NNNNN.json   (exact API bytes)
        |  03_bronze_load      (Bundle -> 1 row per resource, + metadata)
        v
BRONZE  bronze_patient | bronze_encounter | bronze_observation | bronze_condition      (Delta, append-only, partitioned by load_date)
        |  04_silver_scd2      (flatten, clean, dedupe, hash, SCD2 MERGE)
        v
SILVER  silver_patient | silver_encounter | silver_observation | silver_condition      (Delta, SCD Type 2)
        |  05_gold_views
        v
GOLD    gold_dim_patient, gold_fact_* and reporting views                               (views on current versions)

OPS     ops_api_call_log | ops_pipeline_run_log | ops_watermark                          (metadata / versioning)
```

Default location: catalog `fhirapidata1_ws`, schema `fhir_lakehouse`, volume `raw` (all parameterised).

## 2. Repository layout

| Path | Purpose |
| --- | --- |
| `notebooks/00_config` | Widgets/parameters, resource registry (ingestion order + Silver column mappings), shared helpers (watermarks, run log). Included via `%run`. |
| `notebooks/01_setup` | Idempotent bootstrap: schema, Raw volume + folders, ops tables. |
| `notebooks/02_raw_ingest_api` | Incremental, paginated API extraction for one resource; stores responses as-is; logs each call. |
| `notebooks/03_bronze_load` | Converts new Raw JSON pages to Bronze Delta rows with metadata. |
| `notebooks/04_silver_scd2` | Generic flatten/clean/dedupe + SCD Type 2 merge for one resource. |
| `notebooks/05_gold_views` | Creates the Gold star-schema and reporting views. |
| `notebooks/06_validation_checks` | Reconciliation and SCD2 integrity checks (fails the job on error). |
| `notebooks/99_reset_environment` | Manual, destructive wipe for a fresh full reload (needs `confirm_reset=YES`). |
| `job/fhir_medallion_job.json` | Job definition (tasks, parameters, schedule). |

## 3. Ingestion design (Task 1)

* **Incremental window**: per resource, `ops_watermark(layer='raw')` stores the last fully-ingested UTC day. Next run starts the day after. First run = last `lookback_days` (default 3) full days up to yesterday. `start_date` / `end_date` parameters override for backfills.
* **Date bucketing**: each day is requested with `_lastUpdated=ge<day>T00:00:00Z&_lastUpdated=lt<day+1>T00:00:00Z`. Instant precision is required: date-only values are matched fuzzily by HAPI and produce overlapping buckets.
* **Pagination**: `_count=<page_size>` (default 200); the notebook follows `Bundle.link[relation=next]` until absent or `max_pages_per_day` is reached.
* **Resilience**: retries with exponential backoff on 429/5xx/network errors; watermark advances only after a day completes, so failures are re-fetched on the next run.
* **Raw storage**: response bytes written unchanged, one file per page, folder per resource and `load_date`.
* **Order**: Patient -> Encounter -> Observation -> Condition (enforced by Job task dependencies).

## 4. Metadata & versioning

| Requirement | Where |
| --- | --- |
| `extraction_timestamp` | Bronze, Silver and Gold column (= time the API call was made); Gold aggregates expose `latest_extraction_timestamp` |
| `api_url_or_params` | Bronze, Silver and Gold dimension/fact column (exact URL incl. query/next-page params); `ops_api_call_log.request_params` holds the logical params as JSON |
| When each API was called | `ops_api_call_log.api_called_at`, `response_received_at`, `http_status`, `attempts` |
| When data was saved | `ops_api_call_log.file_saved_at` (Raw), `bronze_ingested_at` (Bronze), `silver_loaded_at` / `silver_updated_at` (Silver) |
| Run lineage | `run_id` (= Job run id) on Raw folders, ops tables, `ingest_run_id`/`bronze_run_id`/`silver_run_id` columns |
| Daily change detection + history | SCD Type 2 in Silver (below) |
| Delta time travel | All Bronze/Silver tables are Delta (`DESCRIBE HISTORY`, `VERSION AS OF`) |

### SCD Type 2 (Silver)
* Business key: `patient_id` / `encounter_id` / `observation_id` / `condition_id` (FHIR logical id).
* `record_hash` = SHA-256 over all business columns (FHIR `meta` excluded, so a version bump without content change does not create a new version).
* Columns: `effective_from` (= FHIR `meta.lastUpdated`), `effective_to`, `is_current`, `silver_loaded_at`, `silver_updated_at`, `silver_run_id`.
* MERGE logic: new key -> insert; hash changed and record not older than current -> close current row (`effective_to = new effective_from`, `is_current = false`) and insert the new version; unchanged -> no-op. Multiple versions of an id within one batch are applied in `last_updated` order so no intermediate version is lost.
* Dedupe: rows repeated across pages/runs collapse on (`key`, `effective_from`), keeping the latest extraction.

## 5. Table relationships (Gold)

```
gold_dim_patient (patient_id) 1 ---- * gold_fact_encounter (encounter_id, patient_id)
        |                                      | 1
        | 1                                    |
        * ---- gold_fact_observation (observation_id, patient_id, encounter_id) * 
        * ---- gold_fact_condition   (condition_id,  patient_id, encounter_id) *
```

* Joins: `patient_id` parsed from `subject.reference` (`Patient/<id>`), `encounter_id` from `encounter.reference` (`Encounter/<id>`).
* Facts are LEFT joined to dimensions; `has_patient_dim` / `has_encounter_fact` flag references to resources last updated outside the ingested window (common on a public sandbox).

| Gold view | Grain / use |
| --- | --- |
| `gold_dim_patient` | Patient demographics, age band |
| `gold_fact_encounter` | Encounter with duration, date |
| `gold_fact_observation` | Observation values (quantity/string/concept) |
| `gold_fact_condition` | Condition with clinical status, active flag |
| `gold_patient_clinical_summary` | Per-patient encounter/observation/condition counts |
| `gold_encounter_daily_summary` | Encounters by day, class, status |
| `gold_top_conditions` | Condition frequency and distinct patients |
| `gold_observation_code_stats` | Observation code stats (avg/min/max) |
| `gold_scd2_version_summary` | Versioning KPIs per resource |
| `gold_ingestion_audit_summary` | API calls/entries/bytes per resource and day |

## 6. Orchestration

Job **FHIR Medallion Lakehouse Pipeline** (serverless):

```
setup -> raw_patient -> bronze_patient -> raw_encounter -> bronze_encounter
      -> raw_observation -> bronze_observation -> raw_condition -> bronze_condition
      -> [silver_patient | silver_encounter | silver_observation | silver_condition] (parallel)
      -> gold_views -> validation_checks
```

Job parameters (pushed to every task): `catalog`, `schema`, `raw_volume`, `fhir_base_url`, `page_size`, `lookback_days`, `max_pages_per_day`, `start_date`, `end_date`, `run_id={{job.run_id}}`. Each task sets only `resource`. Daily schedule 06:00 UTC (created paused).

## 7. How to run

1. Clone the repo into a Databricks Git folder.
2. Create the job from `job/fhir_medallion_job.json` (update notebook paths to your Git folder) or run notebooks manually in the order above.
3. Run the job. Backfill: set `start_date`/`end_date`. Fresh reload: run `99_reset_environment` with `confirm_reset=YES`.
4. Query Gold views, or connect Power BI to the SQL warehouse (optional 1-page report).

## 8. Optional extensions
* XML: add `_format=xml`, land `.xml` files in Raw, parse with `from_xml` in Bronze; the Silver/Gold layers are unchanged.
* Power BI: import the Gold views (star schema above) via the Databricks connector.
