# Databricks notebook source
# DBTITLE 1,About
# MAGIC %md
# MAGIC ## 02_raw_ingest_api - Raw layer (incremental, paginated)
# MAGIC Fetches one FHIR resource from the API for a date window and lands every page **as-is** (exact response bytes) in the Raw volume:
# MAGIC
# MAGIC `/Volumes/<catalog>/<schema>/<raw_volume>/<resource>/load_date=YYYY-MM-DD/run_id=<run_id>/page_00001.json`
# MAGIC
# MAGIC * Window: `start_date`/`end_date` params if given; otherwise from the `raw` watermark (+1 day); first run = last `lookback_days` days up to yesterday (UTC).
# MAGIC * Each day is queried with `_lastUpdated=ge<day>T00:00:00Z&_lastUpdated=lt<day+1>T00:00:00Z&_count=<page_size>` (UTC day buckets) and paginated by following the bundle `next` link.
# MAGIC * Every call is logged to `ops_api_call_log` (URL/params, called-at, saved-at, status, entries, file path).

# COMMAND ----------

# DBTITLE 1,Load config
# MAGIC %run ./00_config

# COMMAND ----------

# DBTITLE 1,Parameters and load window
import os, requests

dbutils.widgets.text("resource", "Patient")
RESOURCE = validate_resource(dbutils.widgets.get("resource").strip())
started_at = dt.datetime.utcnow()

today = dt.datetime.utcnow().date()
if START_DATE:
    window_start = dt.date.fromisoformat(START_DATE)
else:
    wm = get_watermark("raw", RESOURCE)
    window_start = (dt.date.fromisoformat(wm) + dt.timedelta(days=1)) if wm else today - dt.timedelta(days=LOOKBACK_DAYS)
window_end = dt.date.fromisoformat(END_DATE) if END_DATE else today - dt.timedelta(days=1)  # inclusive, full days only

days = [window_start + dt.timedelta(days=i) for i in range((window_end - window_start).days + 1)]
print(f"{RESOURCE}: window {window_start} -> {window_end} ({len(days)} day buckets)")

# COMMAND ----------

# DBTITLE 1,HTTP client with retries
session = requests.Session()
session.headers.update({"Accept": "application/fhir+json"})

def http_get(url):
    """GET with exponential backoff on 429/5xx/network errors. Returns (response|None, attempts, error)."""
    err = None
    for attempt in range(1, HTTP_MAX_RETRIES + 1):
        try:
            resp = session.get(url, timeout=HTTP_TIMEOUT_SEC)
            if resp.status_code == 200:
                return resp, attempt, None
            err = f"HTTP {resp.status_code}: {resp.text[:300]}"
            if resp.status_code < 500 and resp.status_code != 429:
                return resp, attempt, err   # non-retryable client error
        except requests.RequestException as e:
            resp, err = None, f"{type(e).__name__}: {e}"
        time.sleep(2 ** attempt)
    return resp, HTTP_MAX_RETRIES, err

# COMMAND ----------

# DBTITLE 1,Paginated fetch per day bucket
LOG_SCHEMA = ("log_id string, run_id string, resource_type string, load_date date, page_number int, "
              "api_url_or_params string, request_params string, http_status int, entry_count int, "
              "api_called_at timestamp, response_received_at timestamp, file_saved_at timestamp, "
              "raw_file_path string, response_bytes long, attempts int, status string, error_message string")

def ingest_day(day):
    # Instant precision (UTC) gives exact, non-overlapping day buckets; date-only values are fuzzy on HAPI.
    params = {"_lastUpdated": [f"ge{day.isoformat()}T00:00:00Z", f"lt{(day + dt.timedelta(days=1)).isoformat()}T00:00:00Z"],
              "_count": PAGE_SIZE, "_format": "json"}
    url = requests.Request("GET", f"{FHIR_BASE_URL}/{RESOURCE}", params=params).prepare().url
    folder = f"{RAW_ROOT}/{RESOURCE.lower()}/load_date={day.isoformat()}/run_id={RUN_ID}"
    os.makedirs(folder, exist_ok=True)

    logs, page = [], 1
    while url and page <= MAX_PAGES:
        called_at = dt.datetime.utcnow()
        resp, attempts, err = http_get(url)
        received_at = dt.datetime.utcnow()
        rec = dict(log_id=str(uuid.uuid4()), run_id=RUN_ID, resource_type=RESOURCE, load_date=day, page_number=page,
                   api_url_or_params=url, request_params=json.dumps(params), http_status=resp.status_code if resp is not None else None,
                   entry_count=0, api_called_at=called_at, response_received_at=received_at, file_saved_at=None,
                   raw_file_path=None, response_bytes=0, attempts=attempts, status="FAILED", error_message=err)
        if err:
            logs.append(rec)
            return logs, err

        path = f"{folder}/page_{page:05d}.json"
        with open(path, "wb") as fh:          # store the response exactly as returned
            fh.write(resp.content)
        bundle = resp.json()
        rec.update(file_saved_at=dt.datetime.utcnow(), raw_file_path=path, response_bytes=len(resp.content),
                   entry_count=len(bundle.get("entry", [])), status="SUCCESS")
        logs.append(rec)
        url = next((l["url"] for l in bundle.get("link", []) if l.get("relation") == "next"), None)
        page += 1

    if url:
        print(f"WARNING {RESOURCE} {day}: stopped at max_pages_per_day={MAX_PAGES}; remaining pages skipped")
    return logs, None

# COMMAND ----------

# DBTITLE 1,Run ingestion, log calls, advance watermark
summary, failure = {}, None
for day in days:
    logs, err = ingest_day(day)
    if logs:
        spark.createDataFrame([tuple(r[c.split()[0]] for c in LOG_SCHEMA.split(", ")) for r in logs], LOG_SCHEMA) \
             .write.mode("append").saveAsTable(T_API_LOG)
    summary[str(day)] = {"pages": sum(r["status"] == "SUCCESS" for r in logs),
                         "entries": sum(r["entry_count"] for r in logs)}
    print(f"{RESOURCE} {day}: {summary[str(day)]}")
    if err:
        failure = f"{day}: {err}"
        break
    set_watermark("raw", RESOURCE, day.isoformat())   # advance only after a day completes

total_entries = sum(v["entries"] for v in summary.values())
log_run("raw", RESOURCE, started_at, "FAILED" if failure else "SUCCESS", total_entries,
        {"window_start": window_start, "window_end": window_end, "days": summary, "error": failure})
if failure:
    raise RuntimeError(f"Raw ingestion failed for {RESOURCE} on {failure}")
dbutils.notebook.exit(json.dumps({"resource": RESOURCE, "entries": total_entries, "days": summary}))

# COMMAND ----------

