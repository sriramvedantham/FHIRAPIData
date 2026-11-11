# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 00_config - shared configuration and helpers
# MAGIC Loaded by every pipeline notebook with `%run ./00_config`. All environment values are widgets, so Job parameters override them (no hardcoding in the layer notebooks).
# MAGIC
# MAGIC Contents:
# MAGIC * Runtime parameters (catalog, schema, volume, API base URL, paging, lookback window)
# MAGIC * `RESOURCES` registry: ingestion order + Silver column mappings per FHIR resource
# MAGIC * Helpers: table naming, watermarks, pipeline run logging

# COMMAND ----------

# DBTITLE 1,Parameters
import json, time, uuid, datetime as dt
from pyspark.sql import functions as F

# ---- Runtime parameters (overridable from Job parameters) ----
dbutils.widgets.text("catalog", "fhirapidata1_ws")
dbutils.widgets.text("schema", "fhir_lakehouse")
dbutils.widgets.text("raw_volume", "raw")
dbutils.widgets.text("fhir_base_url", "https://hapi.fhir.org/baseR4")
dbutils.widgets.text("page_size", "200")
dbutils.widgets.text("lookback_days", "3")
dbutils.widgets.text("max_pages_per_day", "100")
dbutils.widgets.text("start_date", "")   # optional override, YYYY-MM-DD
dbutils.widgets.text("end_date", "")     # optional override, YYYY-MM-DD (inclusive)
dbutils.widgets.text("run_id", "")

CATALOG       = dbutils.widgets.get("catalog")
SCHEMA        = dbutils.widgets.get("schema")
RAW_VOLUME    = dbutils.widgets.get("raw_volume")
FHIR_BASE_URL = dbutils.widgets.get("fhir_base_url").rstrip("/")
PAGE_SIZE     = int(dbutils.widgets.get("page_size"))
LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
MAX_PAGES     = int(dbutils.widgets.get("max_pages_per_day"))
START_DATE    = dbutils.widgets.get("start_date").strip() or None
END_DATE      = dbutils.widgets.get("end_date").strip() or None
RUN_ID        = dbutils.widgets.get("run_id").strip() or f"manual_{dt.datetime.utcnow():%Y%m%d%H%M%S}"

RAW_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{RAW_VOLUME}"
HTTP_TIMEOUT_SEC = 120
HTTP_MAX_RETRIES = 4

def tbl(name: str) -> str:
    """Fully qualified table name inside the project schema."""
    return f"{CATALOG}.{SCHEMA}.{name}"

# Ops / metadata tables
T_API_LOG   = tbl("ops_api_call_log")
T_RUN_LOG   = tbl("ops_pipeline_run_log")
T_WATERMARK = tbl("ops_watermark")

# COMMAND ----------

# DBTITLE 1,Resource registry and Silver mappings
# Ingestion order required by the assignment: Patient -> Encounter -> Observation -> Condition.
# Silver mappings are SQL expressions evaluated against the parsed VARIANT column `r`.

def _ts(path):   # FHIR dateTime/instant -> TIMESTAMP (UTC)
    return f"try_cast({path}::string AS TIMESTAMP)"

def _ref(path, rtype):  # 'Patient/123' -> '123'
    return f"nullif(regexp_extract({path}::string, '{rtype}/([^/]+)', 1), '')"

def _cc(path):  # CodeableConcept -> display text
    return f"coalesce({path}.text::string, {path}.coding[0].display::string)"

RESOURCES = {
    "Patient": {
        "key": "patient_id",
        "columns": {
            "patient_id":            "r:id::string",
            "mrn":                   "r:identifier[0].value::string",
            "identifier_system":     "r:identifier[0].system::string",
            "family_name":           "trim(r:name[0].family::string)",
            "given_name":            "trim(array_join(try_cast(r:name[0].given AS ARRAY<STRING>), ' '))",
            "gender":                "lower(r:gender::string)",
            "birth_date":            "try_cast(r:birthDate::string AS DATE)",
            "is_deceased":           "coalesce(try_cast(r:deceasedBoolean AS BOOLEAN), r:deceasedDateTime IS NOT NULL)",
            "deceased_datetime":     _ts("r:deceasedDateTime"),
            "is_active":             "try_cast(r:active AS BOOLEAN)",
            "phone":                 "r:telecom[0].value::string",
            "address_line":          "r:address[0].line[0]::string",
            "city":                  "initcap(trim(r:address[0].city::string))",
            "state":                 "upper(trim(r:address[0].state::string))",
            "postal_code":           "r:address[0].postalCode::string",
            "country":               "upper(trim(r:address[0].country::string))",
            "marital_status":        _cc("r:maritalStatus"),
            "language":              "r:communication[0].language.coding[0].code::string",
            "managing_org_ref":      "r:managingOrganization.reference::string",
        },
    },
    "Encounter": {
        "key": "encounter_id",
        "columns": {
            "encounter_id":          "r:id::string",
            "patient_id":            _ref("r:subject.reference", "Patient"),
            "status":                "lower(r:status::string)",
            "class_code":            "r:class.code::string",
            "class_display":         "r:class.display::string",
            "type_code":             "r:type[0].coding[0].code::string",
            "type_display":          _cc("r:type[0]"),
            "service_type":          _cc("r:serviceType"),
            "priority":              _cc("r:priority"),
            "period_start":          _ts("r:period.start"),
            "period_end":            _ts("r:period.end"),
            "reason":                _cc("r:reasonCode[0]"),
            "practitioner_ref":      "r:participant[0].individual.reference::string",
            "location_ref":          "r:location[0].location.reference::string",
            "service_provider_ref":  "r:serviceProvider.reference::string",
        },
    },
    "Observation": {
        "key": "observation_id",
        "columns": {
            "observation_id":        "r:id::string",
            "patient_id":            _ref("r:subject.reference", "Patient"),
            "encounter_id":          _ref("r:encounter.reference", "Encounter"),
            "status":                "lower(r:status::string)",
            "category_code":         "r:category[0].coding[0].code::string",
            "code_system":           "r:code.coding[0].system::string",
            "code":                  "r:code.coding[0].code::string",
            "code_display":          _cc("r:code"),
            "effective_datetime":    f"coalesce({_ts('r:effectiveDateTime')}, {_ts('r:effectivePeriod.start')}, {_ts('r:effectiveInstant')})",
            "issued":                _ts("r:issued"),
            "value_quantity":        "try_cast(r:valueQuantity.value AS DOUBLE)",
            "value_unit":            "coalesce(r:valueQuantity.unit::string, r:valueQuantity.code::string)",
            "value_string":          "r:valueString::string",
            "value_concept":         _cc("r:valueCodeableConcept"),
            "value_boolean":         "try_cast(r:valueBoolean AS BOOLEAN)",
            "interpretation_code":   "r:interpretation[0].coding[0].code::string",
            "component_count":       "coalesce(array_size(try_cast(r:component AS ARRAY<VARIANT>)), 0)",
        },
    },
    "Condition": {
        "key": "condition_id",
        "columns": {
            "condition_id":          "r:id::string",
            "patient_id":            _ref("r:subject.reference", "Patient"),
            "encounter_id":          _ref("r:encounter.reference", "Encounter"),
            "clinical_status":       "lower(r:clinicalStatus.coding[0].code::string)",
            "verification_status":   "lower(r:verificationStatus.coding[0].code::string)",
            "category_code":         "r:category[0].coding[0].code::string",
            "code_system":           "r:code.coding[0].system::string",
            "code":                  "r:code.coding[0].code::string",
            "code_display":          _cc("r:code"),
            "severity":              _cc("r:severity"),
            "onset_datetime":        f"coalesce({_ts('r:onsetDateTime')}, {_ts('r:onsetPeriod.start')})",
            "abatement_datetime":    f"coalesce({_ts('r:abatementDateTime')}, {_ts('r:abatementPeriod.start')})",
            "recorded_date":         _ts("r:recordedDate"),
        },
    },
}

# Columns common to every Silver table (FHIR meta). Not part of the change-detection hash.
META_COLUMNS = {
    "version_id":   "r:meta.versionId::string",
    "last_updated": _ts("r:meta.lastUpdated"),
    "meta_source":  "r:meta.source::string",
}

def bronze_table(resource): return tbl(f"bronze_{resource.lower()}")
def silver_table(resource): return tbl(f"silver_{resource.lower()}")

def validate_resource(resource):
    if resource not in RESOURCES:
        raise ValueError(f"Unknown resource '{resource}'. Expected one of {list(RESOURCES)}")
    return resource

# COMMAND ----------

# DBTITLE 1,Metadata helpers (watermarks and run log)
def get_watermark(layer, resource):
    rows = spark.sql(f"""SELECT watermark_value FROM {T_WATERMARK}
                         WHERE layer = '{layer}' AND resource_type = '{resource}'""").collect()
    return rows[0][0] if rows else None

def set_watermark(layer, resource, value):
    spark.sql(f"""
      MERGE INTO {T_WATERMARK} t
      USING (SELECT '{layer}' AS layer, '{resource}' AS resource_type, '{value}' AS watermark_value) s
      ON t.layer = s.layer AND t.resource_type = s.resource_type
      WHEN MATCHED THEN UPDATE SET watermark_value = s.watermark_value, updated_at = current_timestamp(), run_id = '{RUN_ID}'
      WHEN NOT MATCHED THEN INSERT (layer, resource_type, watermark_value, updated_at, run_id)
                            VALUES (s.layer, s.resource_type, s.watermark_value, current_timestamp(), '{RUN_ID}')""")

def log_run(layer, resource, started_at, status, rows_processed=0, details=None):
    """Append one row per notebook execution to the pipeline run log."""
    rec = [(RUN_ID, layer, resource, started_at, dt.datetime.utcnow(), status,
            int(rows_processed), json.dumps(details or {}, default=str))]
    (spark.createDataFrame(rec, "run_id string, layer string, resource_type string, started_at timestamp, "
                                "finished_at timestamp, status string, rows_processed long, details string")
          .write.mode("append").saveAsTable(T_RUN_LOG))

print(f"Config loaded: {CATALOG}.{SCHEMA} | raw={RAW_ROOT} | api={FHIR_BASE_URL} | run_id={RUN_ID}")

# COMMAND ----------

