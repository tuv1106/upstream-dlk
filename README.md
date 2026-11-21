# Upstream DataLake

A local bronze / silver / gold DataLake built from the Upstream vehicle messages API, orchestrated with Prefect and processed with Polars (lazy API).

## Run

```bash
docker run -p 9900:9900 upstream-interview   # the API, wait for "Server Started"
uv sync
uv run dlk                                   # bronze -> silver -> gold
uv run dlk --run-id 20261121T101500123456Z   # resume a failed bronze run
uv run pytest
uv run ruff check src tests
```

The environment is chosen with `--env` and read from `config/<env>.toml`. `local` writes to `./data`. An `s3` example exists (`uv sync --extra s3`), but it isn't exercised.

## Bonus: SQL injection report

```bash
uv run dlk-sqli --column vin --column model --regex "('(''|[^'])*')|(;)"   # Polars, writes gold/sql_injection_report/
uv run dlk-sqli --column vin --regex ";" --print-trino-sql                # same report as one Trino query
```

On bronze (raw values). Output: one row per violating message and column, with the full original message, `violating_column` and `matched_regexes`. Unknown columns fail; non-text columns are skipped with a warning; regexes are used as given (case-sensitive).

How it saves CPU:
- **Regex on distinct values only.** Values repeat a lot (10K rows, ~23 vins), so the regex runs once per distinct value; the full messages are fetched afterwards with `col IN (bad values)`, a filter on a tiny set.
- **One partition at a time** in Polars, so memory stays bounded.
- **Linear-time regex engines.** Polars uses Rust `regex`; on Trino set `regex-library=RE2J`. Both rule out catastrophic backtracking (ReDoS).
- **At scale use Trino, not Polars.** Polars is one machine; Trino spreads the full-table scan over workers, broadcasts the small `violations` side and pushes its values into the bronze scan (dynamic filtering).
- Further ideas, not implemented: a cheap literal prefilter (`'`, `;`, keywords via `str.contains_any`) before the full regex; scanning only new partitions; caching verdicts for values already checked.

## Layout

```
data/
  _staging/<run_id>/raw.json             landed API response, until published
  bronze/_raw/<run_id>.json              archived raw API response (kept raw_retention_days)
  bronze/date=YYYY-MM-DD/hour=H/<run_id>.parquet
  bronze/_runs/<run_id>.json             published runs: run id + touched partitions
  quarantine/bronze/<run_id>.parquet     messages without a timestamp
  silver/date=YYYY-MM-DD/hour=H/part-0.parquet
  silver/_quality/<run_id>.json          data quality report per build
  gold/vin_last_state/data.parquet
  gold/fastest_vehicles_per_hour/date=YYYY-MM-DD/part-0.parquet
  gold/sql_injection_report/<time>.parquet
```

```
src/upstream_dlk/
  config.py        settings per environment
  storage.py       Storage base class + LocalStorage / S3Storage
  api.py           API client
  tables.py        PartitionedTable, SingleFileTable, Run, RunLog
  schema.py        shared column names and types
  layers/          BronzeLayer, SilverLayer, GoldLayer and the report logic
  flows.py         Prefect flows and tasks
```

## How it works

- **Bronze** keeps the raw messages untouched. It only adds `date` and `hour` (UTC, from `timestamp`) and `ingested_at`. Each run lands the raw response, stages parquet, moves it in, then records the run in `_runs`. The run (id + touched partitions) is passed to silver and gold. Every step is safe to retry, and a retry never calls the API again.
- **Silver** rebuilds only the partitions the bronze run touched, reading all bronze files of those partitions. It drops null vins and exact duplicates, strips `manufacturer` and `model`, maps gears to integers (`NEUTRAL`→0, `REVERSE`→-1, unknown→null), and nulls velocities above 300 km/h, flagging them with `is_velocity_suspect`. Columns are renamed to snake_case and rows are sorted by `(vin, timestamp)` for row-group skipping.
- **Gold** updates its reports from the same partitions:
  - `vin_last_state`: an incremental merge of the previous report with the touched partitions. Each state column keeps its own last non-null value, plus the time it was reported (`door_ts`, `wipers_ts`). Values reported at the same timestamp are all kept, as a sorted list.
  - `fastest_vehicles_per_hour`: max velocity per vin per hour, `rank() <= 10`, so vins tied at 10th place are all kept.

Every decision, with the alternatives considered, is in [docs/decisions.md](docs/decisions.md).

## Known limits

- **Data loss window.** The API returns only the latest N messages and has no cursor. The message rate is unknown and may spike, so messages that arrive between two polls beyond N are lost. Polling more often reduces the risk but can't remove it. The real fix is a source that retains messages (e.g. Kafka with offsets). A "no overlap" warning was rejected because, when runs don't overlap naturally, it fires on every run and nobody acts on it.
- **Hive, not Iceberg.** Swapping a partition leaves a gap of milliseconds, and file skipping needs one parquet footer read per file. Iceberg removes both. The triggers to migrate are heavy late data, many files per partition, and vin lookups at scale.
- **Synchronous layers.** Bronze, silver and gold run in one pipeline on the partitions bronze touched. Every layer is idempotent: if silver or gold fails, resume the pipeline with `dlk --run-id <run>` (bronze is skipped for a published run, silver and gold rerun on its partitions), or rerun that flow from Prefect with its saved parameters. A later run does not pick up an earlier failed one. Decoupled layers would need a management table or a queue (e.g. SQS), which is out of scope here.
- Retries: tasks retry only transient errors (network, HTTP 5xx, disk IO). Data errors such as bad JSON fail immediately; fix the cause, then resume with `--run-id`.
- Stage parses the raw JSON twice (schema-drift check, then the typed read). Fine at 10K messages.
- A crash while writing `raw.json` leaves a truncated file that blocks that run id. Very rare; delete it and start a new run.
- One pipeline runs at a time. Concurrent runs would share the silver and gold staging folders.
- If the silver rules change, `vin_last_state` must be rebuilt (delete it and rerun gold over all partitions), because a merge can't un-learn rows.
