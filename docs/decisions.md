# Design decisions log (draft, fed into the design doc + README)

## 1. Engine: Polars (lazy)
- Picked: Polars lazy API. Columnar like parquet, lazy plans, predicate/projection pushdown, no JVM.
- Alternatives: PySpark (industry standard, heavy for 10K rows, needs Java), pandas (clunky for per-vin windows and partitioned writes), DuckDB SQL.
- Efficiency rules: `scan_parquet` not `read_parquet`; native expressions only (no `map_elements`/loops); one `.collect()` per step.
- Review line: company uses Trino = SQL over parquet; Polars lazy is the same idea locally. Spark if data outgrows one machine; layers stay the same.

## 2. Orchestration: Prefect
- Company uses it. Each step a `@task` (with retries), each layer a `@flow`, one parent `pipeline_flow`.

## 3. Bronze
- Raw values untouched. Added columns only: `date` (YYYY-MM-DD), `hour` (Int8, path `hour=7` not `07`), both UTC from `timestamp` ms, plus `ingested_at`.
- Why derived partition columns: Hive prunes only on partition columns. A filter on raw `timestamp` does not prune. (Iceberg hidden partitioning `hours(ts)` would solve this; not enough reason alone to pick Iceberg.)
- `date` + `hour` vs single `date_hour`: both prune; two levels make whole-day filters simpler and match spec wording. Note: `year` column is car model year, not event date.
- `batch_id` dropped: `ingested_at` already identifies a run.
- Layout: Hive `bronze/date=…/hour=…/`, append only, one new file per partition per run.
- Staged run (landing zone):
  1. fetch → `_staging/<run_id>/raw.json` (exact API bytes; avoids schema inference on all-null columns)
  2. transform → staging parquet (on failure: rerun, overwrite)
  3. publish: move into `bronze/` (atomic rename locally), archive raw to `bronze/_raw/<run_id>.json`, commit to run log, delete run folder
  - Retry with same run_id skips completed steps → never re-fetches.
  - File per step, not file per message (10K tiny files = small-file problem).
- Rejected: resume-from-offset / management table. API has no cursor; window moves between calls, so row N differs; key checkpoint `(ts, vin)` drops late data. Right answer for paginated/Kafka source, not this one.
- Overlap between runs → dedupe in silver.

### Data-loss risk (README)
- API returns only latest N, no cursor. Message rate unknown and may spike (10K/min then 20K/min). If more than N arrive between polls, messages are lost forever.
- With a known rate you can size polling + retries; with unknown rate you cannot guarantee coverage.
- Real fix: a retaining source (Kafka with offsets, replay).
- Rejected: overlap check warning ("newest bronze ts < oldest new ts"). It fires every run when runs don't overlap naturally → noise nobody acts on.

## 4. Silver
- Data facts (10K sample): `" Buick"` has LEADING space (spec says trailing); one `model` too; about 440 null vins per 10K (the API generates fresh data every call); gears `0-6, -1, NEUTRAL, REVERSE, null`; `vin+timestamp` not unique (33 pairs differ in location/speed/gear); 0 full-row duplicates; 23 distinct vins.
- Clean: `strip` on `manufacturer` and `model`; drop null vin.
- Dedupe key: full row (all original API columns, ignoring `ingested_at/date/hour`). Rejected `vin+timestamp`: would delete 33 real messages.
- Gear map: `1-6`→1-6, `0`/`NEUTRAL`→0, `-1`/`REVERSE`→-1, null→null (unknown ≠ neutral), other text→null + logged count. Data is synthetic (gear doesn't correlate with speed), so mapping uses domain logic + the paired spellings.
- Velocity outlier: one vin (Toyota Camry 2015) reports exactly 450 on all 398 messages, incl. REVERSE. Others ≤190 (mean 70, std 42). Not units (mph would be 724 km/h; fleet fits km/h). Stuck sensor.
  - Rule: velocity > 300 km/h = physically implausible (fixed threshold, not z-score, which shifts with data).
  - Handling: set velocity null + `is_velocity_suspect = true`; keep row (door/wipers still valid). Original kept in bronze. DQ report lists flagged vins, counts, reason.
  - Null not -1: sentinel pollutes avg/min; null is skipped by aggregates.
- Partitioning: same `date/hour`. Duplicates share timestamp → same partition → dedupe per partition. Late data → only its old hour reprocessed.
- Sort inside files by `(vin, timestamp)`: parquet row-group min/max skipping on vin; vin prefix (WMI) ≈ manufacturer, so rough manufacturer clustering free; better compression.
  - Rejected: partition by vin (millions of folders), by manufacturer (skewed), hash buckets (many small files), Z-order (needs Iceberg/Delta).
- Write: overwrite only touched partitions (list passed from bronze run). Staging + swap (rename old→`_old`, new→target, delete old). Rejected: full rebuild each run (doesn't scale).
- No partition versioning (`date/hour/version`): readers would need a "current" pointer = hand-rolled Iceberg snapshots. Rollback = rerun silver from immutable bronze.

## 5. Hive vs Iceberg (the dilemma, for the interview)
- Picked Hive for the exercise; Iceberg for production.
- How pruning works:
  - Hive: metastore knows partitions only (+ optional row-count stats). Per-file min/max lives in each parquet footer → engine must open every file's footer → row-group skipping only.
  - Iceberg: manifest list holds partition-value ranges per manifest (date/hour only, no vin). Manifests hold per-file column stats (vin min/max) → skips files without opening them. No "vin per day" rollup; still a per-file metadata check.
- Catch for both: min/max skipping only works if a file covers a narrow vin range. One hour file with millions of vins has min≈`1A…`, max≈`ZZ…` → no skipping. Needs sort + split into vin-range files (Iceberg sort order + target file size does it) or parquet bloom filters on vin.
- Iceberg pros: file skipping via manifests, atomic overwrite (no swap gap), managed sort order, compaction, vin bucketing, Trino-native.
- Iceberg cons here: local setup (pyiceberg + SQLite catalog), more code (graded on conciseness), snapshot storage cost on big tables.
- Hive cons: swap has a millisecond gap; one footer read per file (costly on S3 with many files).
- Migration triggers: heavy late data / frequent rewrites, many files per partition, vin lookups at scale.
- `(vin, timestamp)` sort keeps layout migration-ready.

## 6. Handing work between layers: synchronous flow
- Picked: one Prefect pipeline. `bronze_flow` returns the run (run id + touched partitions); `silver_flow(run_id, partitions)` and `gold_flow(partitions)` run on exactly those partitions.
- Run log `bronze/_runs/<run_id>.json` = `{run_id, partitions}`, written last in publish. Used only as the "published" marker for resume, and to rerun silver/gold for a published run (`dlk --run-id X`).
- Failure handling: silver and gold are idempotent (rebuild whole partitions; merge is max + set union). On failure: resume the pipeline with `dlk --run-id <run>` (skips bronze for a published run, reruns silver/gold on its partitions), or rerun the failed flow from Prefect with its saved parameters. A later run does not pick up an earlier failed one (accepted trade-off).
- Considered: asynchronous layers, where each layer finds its own pending work (watermark per layer over the run log). It recovers on its own after a failure, but adds state to explain and operate, and needs batching for backlogs (a 20-hour outage arrives as one oversized job). If the layers ever need to run independently, a management table or a queue (e.g. SQS) is the better tool than log files. Out of scope for this task.

## 7. Gold
- `vin_last_state`: no partition (one row per vin). Columns: `vin, last_reported_timestamp, front_left_door_state (list), door_ts, wipers_state (list), wipers_ts`.
  - Spec reading: each state column = its own last non-null value (independent), not "last row where both non-null" (that gives stale values).
  - Same vin + same ts with conflicting values (23 cases): keep all distinct values as a sorted array. Rejected: pick one (loses data), null (loses data), string concat (bool becomes string, parsing, delimiter collisions), exploded rows (breaks one row per vin).
  - Incremental merge: old state turned back into rows (`UNNEST`) + new rows from touched partitions → per vin keep values at max ts → `array_agg(DISTINCT)`. Newer ts replaces, equal ts unions, older ts ignored. Idempotent (max + set-union). Late events land in their old hour → that hour is touched → seen.
  - Limit: state cannot un-learn; if silver logic changes → full rebuild (empty state + all partitions, same code).
  - Rejected: full recompute (simple but scans all silver each run); window `ROW_NUMBER` over all silver (no pruning); CTE of last date/hour per vin (last non-null may sit in an earlier hour).
  - No ingestion date/hour columns: not prunable; the partition list passed from bronze is already exact. `ingested_at` kept in silver for lineage.
- `top10_per_hour`: columns `vin, date, hour, top_velocity`, partition by `date` (hour partitions = 10-row files). Recompute touched days. Suspect velocities (null) excluded.
  - Ties: `rank() <= 10` (top 10 with ties; an hour may return >10 rows). Real data: hour 13 has 3 vins tied at 10th. Rejected: `row_number` (arbitrary cut), `dense_rank` (top 10 speeds, 20+ vins).

## 8. Implementation calls (made while building)
- Silver renames API camelCase to snake_case and converts `timestamp` to `Datetime(ms, UTC)` (standardization).
- Dedupe = group by all API columns + partition, `min(ingested_at)` → deterministic, keeps first ingestion.
- Gear text is stripped + upper-cased before mapping (`neutral` → 0).
- Storage: `Storage` base on fsspec; `LocalStorage` (atomic `os.replace`), `S3Storage` (copy + delete, not atomic). Chosen by `config/<env>.toml`.
- Prefect tasks wrap layer methods (logic stays testable without Prefect); `NO_CACHE` + 3 retries on transient errors only (see §10).
- Observed live: two runs seconds apart had 0 overlapping messages, so the API generates fresh messages per call. Top 10 per hour returned 14 vins, all tied at 190.

## 9. Fixes from external review
- Null `timestamp`: row cannot be partitioned (would create `date=None/`, break silver forever). Quarantined to `quarantine/bronze/<run_id>.parquet` with a warning; never dropped.
- Bronze "as-is": raw API response archived in `bronze/_raw/` (parquet can be rebuilt from it). `velocity` read as Float64 so 12.7 is not truncated to 12. Fields not in the schema are logged as a warning (schema drift), and stay in the raw archive.
- Staging folders deleted after successful silver/gold builds.
- Raw archive retention: `[bronze] raw_retention_days = 30`; last bronze task deletes `_raw/*.json` older than that (age from run id).
- Resume (`--run-id`) vs retry: resume = run id in run log → skip bronze, rerun silver/gold; landed (`raw.json` exists) → continue stage/publish; never landed → fail (that is a new run and needs a new id). Retry = Prefect re-running a failed task inside the same flow run; only there may `land` call the API again.

## 10. Error handling
- Retries only transient errors (`is_transient` in flows.py): httpx transport errors, HTTP 5xx, `OSError`. Data errors (Polars parse errors, `ValueError`) fail fast: retrying gives the same result.
- API down / timeout / 5xx → 3 retries, 10s apart, then flow fails; no raw file written.
- Bad JSON or wrong type → stage fails immediately; raw kept for debugging; resume with `--run-id` after fixing.
- Schema drift (new fields) → warning; raw keeps the field.
- Null timestamp → quarantine. Null vin / unknown gear / velocity > 300 → silver data rules, counted in the quality report.
- Disk / permission errors → retried; writes go through staging, readers see old or new data, never half.
- Resume of a never-landed run, bad config → fail immediately with a clear message.
- Accepted risk: crash while writing `raw.json` leaves a truncated file that blocks that run id (land skips, stage fails). Very rare; fix = delete it and start a new run.

## 11. Bonus: SQL injection report
- Runs on demand (columns and regexes are arguments, not known in advance): `sql_injection_flow`, CLI `dlk-sqli`. Output under `gold/sql_injection_report/<time>.parquet`.
- "Message" = full API record (as in "read 10K messages"), so the report holds full rows + `violating_column` + `matched_regexes`. A message violating in 2 columns gives 2 rows.
- Algorithm: per bronze partition → `SELECT DISTINCT` per text column (`UNION ALL`, via Polars SQL) → regex on distinct values only → fetch full rows with `col IN (bad values)` and join the tiny violations table for `matched_regexes`.
- Rejected: regex on every row (repeats work on repeated values); Python `re` loop (slow, ReDoS risk); loading whole columns (does not scale).
- Engine: Polars for this task; at 100 TB a single machine is read-bound (~14 h at 2 GB/s, rough estimate), so `--print-trino-sql` emits the same report as a Trino query (broadcast join + dynamic filtering).
- Column names from the user are validated against the bronze schema before going into SQL (otherwise our own query would be injectable).
- Publish is retry-safe at every step: staged files, quarantine and raw archive are each moved only if still in staging. Resume treats a run as landed if its raw file is in staging or already archived. `FileNotFoundError` is not retried.
- Raw expiry runs last in the pipeline, so housekeeping never blocks silver or gold.
- Verified on real Trino: `tests/test_trino.py` loads the same rows into Trino's `memory` catalog, runs the generated query, and asserts Trino == Polars == expected. Skipped when no Trino listens on localhost:8080.
- Regex edge cases tested on both engines: keyword alone, lowercase (no match, case-sensitive), keyword inside a word (`SELECTOR`, no match thanks to `\b`), `UNION ALL`, one quote (`it's`, no match) vs a quoted string, `;`, and a value matching several regexes.
- Publish idempotency uses file location as the log (staging = not done, archive = done). A write-ahead log would be the general tool when steps cannot be checked from state, e.g. writes to an external system.
