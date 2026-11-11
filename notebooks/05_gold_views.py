# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 05_gold_views - Gold reporting layer (star schema views)
# MAGIC Views over the **current** SCD2 versions in Silver, ready for BI (Power BI / AI/BI dashboards).
# MAGIC
# MAGIC | View | Grain |
# MAGIC | --- | --- |
# MAGIC | gold_dim_patient | one row per patient |
# MAGIC | gold_fact_encounter | one row per encounter |
# MAGIC | gold_fact_observation | one row per observation |
# MAGIC | gold_fact_condition | one row per condition |
# MAGIC | gold_patient_clinical_summary | one row per patient with activity counts |
# MAGIC | gold_encounter_daily_summary | day x class x status |
# MAGIC | gold_top_conditions | condition code |
# MAGIC | gold_observation_code_stats | observation code x unit |
# MAGIC | gold_scd2_version_summary | resource (history/versioning KPIs) |
# MAGIC | gold_ingestion_audit_summary | resource x load_date (API metadata) |

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Dimension and fact views
started_at = dt.datetime.utcnow()
P, E, O, C = (silver_table(r) for r in ["Patient", "Encounter", "Observation", "Condition"])

views = {}
views["gold_dim_patient"] = f"""
SELECT patient_id, mrn, given_name, family_name,
       nullif(concat_ws(' ', given_name, family_name), '') AS full_name,
       coalesce(gender, 'unknown') AS gender, birth_date,
       CAST(floor(months_between(current_date(), birth_date) / 12) AS INT) AS age_years,
       CASE WHEN birth_date IS NULL THEN 'Unknown'
            WHEN months_between(current_date(), birth_date) / 12 < 18 THEN '0-17'
            WHEN months_between(current_date(), birth_date) / 12 < 40 THEN '18-39'
            WHEN months_between(current_date(), birth_date) / 12 < 65 THEN '40-64'
            ELSE '65+' END AS age_band,
       is_deceased, is_active, city, state, postal_code, country, marital_status, language,
       last_updated, version_id, effective_from AS version_valid_from, record_hash,
       extraction_timestamp, api_url_or_params
FROM {P} WHERE is_current"""

views["gold_fact_encounter"] = f"""
SELECT e.encounter_id, e.patient_id, (p.patient_id IS NOT NULL) AS has_patient_dim,
       e.status, e.class_code, e.class_display, e.type_code, e.type_display, e.service_type, e.priority, e.reason,
       e.period_start, e.period_end, to_date(e.period_start) AS encounter_date,
       timestampdiff(MINUTE, e.period_start, e.period_end) AS duration_minutes,
       e.practitioner_ref, e.location_ref, e.service_provider_ref, e.last_updated,
       e.version_id, e.effective_from AS version_valid_from, e.record_hash, e.extraction_timestamp, e.api_url_or_params
FROM {E} e LEFT JOIN {P} p ON p.patient_id = e.patient_id AND p.is_current
WHERE e.is_current"""

views["gold_fact_observation"] = f"""
SELECT o.observation_id, o.patient_id, o.encounter_id,
       (p.patient_id IS NOT NULL) AS has_patient_dim, (e.encounter_id IS NOT NULL) AS has_encounter_fact,
       o.status, o.category_code, o.code_system, o.code, o.code_display,
       o.effective_datetime, to_date(o.effective_datetime) AS effective_date, o.issued,
       o.value_quantity, o.value_unit, o.value_string, o.value_concept, o.value_boolean,
       o.interpretation_code, o.component_count, o.last_updated,
       o.version_id, o.effective_from AS version_valid_from, o.record_hash, o.extraction_timestamp, o.api_url_or_params
FROM {O} o
LEFT JOIN {P} p ON p.patient_id = o.patient_id AND p.is_current
LEFT JOIN {E} e ON e.encounter_id = o.encounter_id AND e.is_current
WHERE o.is_current"""

views["gold_fact_condition"] = f"""
SELECT c.condition_id, c.patient_id, c.encounter_id,
       (p.patient_id IS NOT NULL) AS has_patient_dim, (e.encounter_id IS NOT NULL) AS has_encounter_fact,
       c.clinical_status, c.verification_status, c.category_code, c.code_system, c.code, c.code_display, c.severity,
       c.onset_datetime, c.abatement_datetime, c.recorded_date,
       (c.clinical_status IN ('active', 'recurrence', 'relapse')) AS is_active_condition, c.last_updated,
       c.version_id, c.effective_from AS version_valid_from, c.record_hash, c.extraction_timestamp, c.api_url_or_params
FROM {C} c
LEFT JOIN {P} p ON p.patient_id = c.patient_id AND p.is_current
LEFT JOIN {E} e ON e.encounter_id = c.encounter_id AND e.is_current
WHERE c.is_current"""

# COMMAND ----------

# DBTITLE 1,Aggregate / reporting views
G = lambda name: tbl(name)

views["gold_patient_clinical_summary"] = f"""
WITH enc AS (SELECT patient_id, count(*) encounters, max(period_start) last_encounter_at FROM {G('gold_fact_encounter')} GROUP BY patient_id),
     obs AS (SELECT patient_id, count(*) observations FROM {G('gold_fact_observation')} GROUP BY patient_id),
     cnd AS (SELECT patient_id, count(*) conditions, count_if(is_active_condition) active_conditions FROM {G('gold_fact_condition')} GROUP BY patient_id)
SELECT p.patient_id, p.full_name, p.gender, p.age_band, p.city, p.state,
       coalesce(enc.encounters, 0) AS encounters, enc.last_encounter_at,
       coalesce(obs.observations, 0) AS observations,
       coalesce(cnd.conditions, 0) AS conditions, coalesce(cnd.active_conditions, 0) AS active_conditions,
       p.version_id, p.version_valid_from, p.extraction_timestamp, p.api_url_or_params
FROM {G('gold_dim_patient')} p
LEFT JOIN enc ON enc.patient_id = p.patient_id
LEFT JOIN obs ON obs.patient_id = p.patient_id
LEFT JOIN cnd ON cnd.patient_id = p.patient_id"""

views["gold_encounter_daily_summary"] = f"""
SELECT encounter_date, coalesce(class_display, class_code, 'unknown') AS encounter_class, coalesce(status, 'unknown') AS status,
       count(*) AS encounters, count(DISTINCT patient_id) AS distinct_patients,
       round(avg(duration_minutes), 1) AS avg_duration_minutes,
       max(extraction_timestamp) AS latest_extraction_timestamp, max(version_valid_from) AS latest_version_valid_from
FROM {G('gold_fact_encounter')}
GROUP BY ALL"""

views["gold_top_conditions"] = f"""
SELECT code, coalesce(code_display, 'unknown') AS condition_name, code_system,
       count(*) AS condition_records, count(DISTINCT patient_id) AS distinct_patients,
       count_if(is_active_condition) AS active_records,
       max(extraction_timestamp) AS latest_extraction_timestamp, max(version_valid_from) AS latest_version_valid_from
FROM {G('gold_fact_condition')}
GROUP BY ALL"""

views["gold_observation_code_stats"] = f"""
SELECT code, coalesce(code_display, 'unknown') AS observation_name, category_code, value_unit,
       count(*) AS observations, count(DISTINCT patient_id) AS distinct_patients,
       count(value_quantity) AS numeric_values,
       round(avg(value_quantity), 2) AS avg_value, min(value_quantity) AS min_value, max(value_quantity) AS max_value,
       min(effective_datetime) AS first_effective, max(effective_datetime) AS last_effective,
       max(extraction_timestamp) AS latest_extraction_timestamp, max(version_valid_from) AS latest_version_valid_from
FROM {G('gold_fact_observation')}
GROUP BY ALL"""

views["gold_scd2_version_summary"] = "\nUNION ALL\n".join(f"""
SELECT '{r}' AS resource_type, count(*) AS total_versions, count_if(is_current) AS current_records,
       count_if(NOT is_current) AS historical_versions,
       count(DISTINCT CASE WHEN NOT is_current THEN {RESOURCES[r]['key']} END) AS entities_changed,
       max(silver_updated_at) AS last_silver_update, max(extraction_timestamp) AS latest_extraction_timestamp
FROM {silver_table(r)}""" for r in RESOURCES)

views["gold_ingestion_audit_summary"] = f"""
SELECT resource_type, load_date, count(*) AS api_calls, count_if(status = 'SUCCESS') AS successful_calls,
       sum(entry_count) AS entries_fetched, sum(response_bytes) AS bytes_saved,
       min(api_called_at) AS first_api_call_at, max(file_saved_at) AS last_file_saved_at,
       count(DISTINCT run_id) AS ingestion_runs
FROM {T_API_LOG}
GROUP BY resource_type, load_date"""

# COMMAND ----------

# DBTITLE 1,Create views
for name, body in views.items():
    spark.sql(f"CREATE OR REPLACE VIEW {tbl(name)} AS {body}")
    print(f"created {tbl(name)}: {spark.table(tbl(name)).count()} rows")

log_run("gold", "ALL", started_at, "SUCCESS", len(views), {"views": list(views)})
display(spark.table(tbl("gold_scd2_version_summary")))

# COMMAND ----------

