# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Upstream Data Engineer home task: build a local DataLake (bronze / silver / gold) from vehicle messages served by a local HTTP API. The full spec is in `Data Engineer Task - Vehicle Messages Http Server.md`. Treat it as the source of truth and re-read it before changing pipeline behavior.

## Commands

```bash
uv sync                      # install (uv, not pip)
uv run dlk [--env local]     # run bronze -> silver -> gold via Prefect (API must be up)
uv run pytest                # tests
uv run ruff check src tests && uv run ruff format src tests
```

Code lives in `src/upstream_dlk/` (see README "Layout"). Design decisions with alternatives: `docs/decisions.md`. Keep it updated when a decision changes.

The reviewers grade the code on being concise, easy to understand and following good practices. Prefer small, readable modules over clever abstractions.

## Data source (external, must be running)

The API runs in a Docker image supplied by Upstream (the download link is in the spec; a separate image exists for Apple Silicon):

```bash
docker load -i upstream_interview.tar
docker run -p 9900:9900 upstream-interview   # ready when it logs "Server Started"
curl "http://localhost:9900/upstream/vehicle_messages?amount=10000"
```

`GET /upstream/vehicle_messages?amount=N` returns a JSON array of recent messages with these fields: `vin`, `manufacturer`, `year`, `model`, `latitude`, `longitude`, `timestamp` (epoch **ms**), `velocity`, `frontLeftDoorState`, `wipersState` (bool), `gearPosition` (string), `driverSeatbeltState`.

## Pipeline architecture

Each layer reads only from the layer before it, and each layer is stored as parquet in its own folder:

1. **Bronze**: raw data. Fetch 10K messages from the API and write them as-is to parquet, partitioned by **date and hour** (derived from `timestamp`).
2. **Silver**: built from bronze. It does three things:
   - strips the trailing whitespace from `manufacturer` (one manufacturer has a trailing space)
   - drops rows with a null `vin`
   - converts `gearPosition` to integers only (the raw data mixes numeric and non-numeric values)
3. **Gold**: reports built from silver.
   - `vin_last_state`: one row per vin, with `vin`, `last_reported_timestamp` (max timestamp), `front_left_door_state` and `wipers_state`. Each state column holds the **last non-null** value for that vin, which is not the same as the value on the latest row.
   - Top 10 fastest vehicles per hour: rank each vin by its max `velocity` within each date-hour. Output `vin`, date-hour and top velocity, ordered from highest to lowest.

**Bonus:** `sqlInjectionReport(columns, regexes)` runs on **bronze** (not silver). It outputs each violating original message together with the name of the column that matched. The spec also asks, as discussion only, how to make regex matching cheaper at scale. No implementation is needed for that part.

## Delivery

The code is committed to a GitHub repo and shared with the Upstream reviewers listed in the spec.
