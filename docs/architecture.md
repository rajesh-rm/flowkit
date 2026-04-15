# Architecture

## Overview

`data_assets` is a self-contained ETL engine for data assets. Apache Airflow calls `run_asset(name, mode)` and the package handles everything: locking, extraction, checkpointing, schema management, validation, promotion, and observability.

**Airflow knows *when* and *whether* to run. The package knows *how*.**

## Layered Design

```
┌─────────────────────────────────────────────────────────────────┐
│                        runner.py                                 │
│  run_asset() — orchestrates the full lifecycle                   │
├─────────┬──────────┬───────────┬──────────┬────────────────────┤
│ extract/│  load/   │checkpoint/│transform/│   observability/   │
│         │          │           │          │                     │
│ API     │ loader:  │ locks     │ SQL      │ logging            │
│ client  │  DDL     │ checkpts  │ transforms│ run_tracker       │
│ rate    │  temp    │           │          │                     │
│ limiter │  promote │           │          │                     │
│ tokens  │          │           │          │                     │
│ parallel│          │           │          │                     │
├─────────┴──────────┴───────────┴──────────┴────────────────────┤
│                     core/ + db/                                  │
│  Asset classes, enums, types, registry, SQLAlchemy models        │
└─────────────────────────────────────────────────────────────────┘
```

## Run Lifecycle

When Airflow calls `run_asset("my_asset", mode="forward")`:

1. **Initialize** — Discover assets, acquire lock, read coverage watermarks, check for retry checkpoints
2. **Extract** — Fetch data via API client or custom `extract()` hook (e.g., pysnc for ServiceNow) into a temp table
3. **Transform & Validate** — Apply `asset.transform(df)`, run `asset.validate(df, context)`
4. **Promote** — Move from temp table to main table via FULL_REPLACE, UPSERT, or APPEND (single transaction)
5. **Finalize** — Update coverage tracker, record metrics, clear checkpoints, drop temp table, release lock

On failure (including Ctrl+C): checkpoints are cleared, temp table is dropped, and lock is released. Each cleanup step is independently guarded — if one fails (e.g., DB unreachable), the remaining steps still run.

## Run Modes

| Mode | When to use |
|------|-------------|
| `full` | Initial load or periodic full refresh |
| `forward` | Incremental — fetch new data since last run |
| `backfill` | Fill in historical data going backwards |
| `transform` | Run SQL transforms (database-to-database) |

### Example: running the same asset across modes

```python
# Day 1: First load — fetches all SonarQube projects
run_asset("sonarqube_projects", run_mode="full")
# forward_watermark → 2026-04-01T12:00:00Z

# Day 2: Incremental — only projects updated since last run
run_asset("sonarqube_projects", run_mode="forward")
# start_date = 2026-04-01T12:00:00Z, end_date = now
# forward_watermark → 2026-04-02T08:00:00Z
```

For which assets support incremental mode, see [assets-catalog.md](assets-catalog.md). For the practical decision tree on choosing a run mode, see [How-To Guides](how-to-guides.md#how-to-choose-a-run-mode). For details on how watermarks work, see [How-To Guides](how-to-guides.md#how-watermarks-work).

## Extraction Data Flow

This diagram shows how data flows through a single extraction cycle:

```
                    ┌──────────────┐
                    │  Runner      │
                    │  run_asset() │
                    └──────┬───────┘
                           │
              ┌────────────▼────────────┐
              │ asset.build_request()    │◄──── checkpoint (page/offset/cursor)
              │ → RequestSpec            │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │ APIClient.request()      │
              │  ├─ rate_limiter.acquire()│
              │  ├─ token_mgr.get_auth() │
              │  └─ httpx.request()      │
              └────────────┬────────────┘
                           │
              ┌────────────▼────────────┐
              │ asset.parse_response()   │
              │ → (DataFrame,            │
              │    PaginationState)       │
              └────────┬────────┬───────┘
                       │        │
          ┌────────────▼──┐ ┌───▼──────────────┐
          │ write_to_temp()│ │ save_checkpoint() │
          │ → temp_store   │ │ → data_ops        │
          └────────────────┘ └──────────────────┘
                       │
                       │  state.has_more?
                       │  YES → loop back to build_request()
                       │  NO  → proceed to transform & validate
```

**Alternative path — `extract()` hook (e.g., ServiceNow/pysnc, SonarQube Projects):**

Assets that override `extract()` bypass the diagram above. The runner calls `asset.extract(engine, temp_table, context)` directly, and the asset handles fetching and writing to the temp table using its own client. Two examples: ServiceNow assets use pysnc with credentials from `ServiceNowTokenManager.get_pysnc_auth()`. SonarQube Projects overrides `extract()` to shard queries via the `q` parameter (working around a 10k Elasticsearch result limit) while reusing `build_request()`/`parse_response()` from `RestAsset` internally:

```
                    ┌──────────────┐
                    │  Runner      │
                    │  run_asset() │
                    └──────┬───────┘
                           │
              ┌────────────▼────────────┐
              │ asset.extract()          │
              │  ├─ create SDK client    │
              │  ├─ iterate records      │
              │  └─ write_to_temp()      │
              └────────────┬────────────┘
                           │
                   proceed to transform
                   & validate
```

## Rate Limiter + Parallel Workers

```
┌─────────────────────────────────────────┐
│          Shared Rate Limiter             │
│  (e.g., 10 calls/sec for the asset)     │
│                                          │
│    ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ │
│    │ W-0  │ │ W-1  │ │ W-2  │ │ W-3  │ │
│    │Thread│ │Thread│ │Thread│ │Thread│ │
│    └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘ │
│       │        │        │        │      │
│       └────────┴────┬───┴────────┘      │
│                     │                    │
│              limiter.acquire()           │
│         (blocks until token available)   │
└─────────────────────────────────────────┘

IMPORTANT: 4 workers at 10/sec = still 10 calls/sec TOTAL, not 40.
The limiter is shared. Workers wait their turn.
```

For the key design decisions table and database schema layout, see [Extending Reference](extending-reference.md#key-design-decisions) and [Configuration](configuration.md#database-schemas).

## Parallel Extraction Modes

All three modes use a shared `_fetch_pages()` loop for the core request→parse→write→checkpoint cycle. The difference is how work units are determined and distributed.

### Sequential (NONE)
Default. One thread. The runner calls `_fetch_pages()` with a `request_builder` that delegates to `asset.build_request(context, checkpoint)`. Each iteration gets the latest checkpoint, so the asset controls the URL and params — supporting multi-endpoint assets (e.g., GitHub repos iterating through orgs).

### Page-Parallel
For endpoints where total pages are discoverable from the first response. Discovery call fetches page 1 and reads `total_pages`. Remaining pages are partitioned across `max_workers` threads (pool size capped at actual partition count). Each worker checkpoints independently. On retry, completed workers are skipped.

**Use when:** the API returns a total count/pages in the first response.

### Entity-Parallel
For child resources (PRs per repo, issues per project). Parent entity keys are loaded from a parent asset's table, partitioned across threads. Each worker calls `_fetch_pages()` per entity with `build_entity_request()`. Entities are marked complete only after all their pages succeed — preventing data loss on partial failure.

**Use when:** you need to fetch sub-resources for each parent entity. Requires `parent_asset_name` referencing an already-loaded asset.

### Shared infrastructure

- **Rate limiter**: one sliding-window instance shared across all threads — 4 workers at 10/sec = 10/sec total
- **Token manager**: thread-safe, shared — single token refreshed for all workers
- **Error handling**: `SkippedRequestError` (e.g., 404, or GitHub 409 for empty repos) skips the entity, doesn't kill the run. Non-JSON responses (e.g., HTML error pages from proxies) are caught and wrapped with URL, status code, and body preview for diagnostics. Assets can override `classify_error()` for source-specific behavior.
- **Thread pool**: `_run_workers()` caps pool size at `min(max_workers, work_units)` — no wasted threads

For the complete catalog of asset definition patterns (RestAsset, APIAsset, shared base classes), see [Extending Reference](extending-reference.md#shared-base-classes).

## Run Resilience

- **UUIDv7 run IDs** — timestamp-ordered, sortable. Each run gets a unique ID that sorts chronologically.
- **Stale-run takeover** — if a worker is killed without cleanup (kill -9, OOM), the next retry detects the abandoned run (no heartbeat in `stale_heartbeat_minutes` (default 20) OR exceeded `max_run_hours` (default 5)), inherits its temp table, and resumes extraction. Clean failures (exceptions, Ctrl+C) clear checkpoints and release the lock immediately — no takeover needed.
- **Secrets injection** — `run_asset(secrets={...})` injects credentials as env vars for the run duration. Cleaned up in `finally` block. Airflow DAGs use this to pass secrets from Connections.
- **Entity-parallel unified checkpoint** — each checkpoint saves completed entities + current entity + pagination position, enabling exact mid-entity resume.
- **Partition isolation** — `partition_key` on `run_asset()` scopes locks, watermarks, and checkpoints to `(asset_name, partition_key)`. Multiple orgs run concurrently without interference.

## Data Quality

- **Column type correctness** — Boolean fields from APIs are stored as native `Boolean()` columns (not Text strings). ServiceNow coordinates use `Float()`. DateTime fields are stored as `DateTime(timezone=True)`. The `_batch_to_df()` method in `ServiceNowTableAsset` coerces all typed columns: string booleans (`"true"`/`"false"`) to Python booleans, coordinate strings to floats, and datetime strings (including empty strings `""` for null values) to proper `datetime64` with UTC. Missing declared columns raise a `ValueError` immediately, preventing silent data loss.
- **Datetime safety net** — The loader's `_coerce_datetime_strings()` provides a universal safety net for all assets. It detects datetime-like string columns by sampling the first non-empty value against a regex pattern (`YYYY-MM-DD[T ]HH:MM`), replaces empty strings with `None`, and converts via `pd.to_datetime(utc=True, errors="coerce")`. This catches both ISO 8601 and ServiceNow's space-separated datetime format.
- **Unique index safety** — During promotion, empty strings in Text columns covered by unique indexes are converted to NULL (PostgreSQL and MariaDB both treat `''` as a regular value that violates UNIQUE, but allow multiple NULLs). The loader then checks for remaining duplicate non-NULL values and logs a WARNING with sample duplicates. If a unique index still fails due to genuine duplicates, it falls back to a non-unique index and logs a warning so the pipeline completes and operators can investigate.
- **Column length validation** — Assets can declare `column_max_lengths` (a dict of column name → max chars). The base `Asset.validate()` method checks these during the validation step and blocks promotion if any value exceeds its limit. Additionally, `Asset.validate_warnings()` warns (non-blocking) if any string column contains values exceeding 10,000 characters.
- **Database connection retry** — The `@db_retry()` decorator (in `db/retry.py`) automatically retries transient database errors on three critical operations: `write_to_temp()`, `promote()`, and `save_checkpoint()`. It retries `OperationalError`, `DisconnectionError`, `ConnectionError`, and `TimeoutError` with exponential backoff (default: 3 attempts, 2s base delay). Non-retryable errors (`IntegrityError`, `ProgrammingError`) fail immediately. Configurable via `DATA_ASSETS_DB_RETRY_ATTEMPTS` and `DATA_ASSETS_DB_RETRY_BASE_DELAY` env vars. On exhaustion, raises `DatabaseRetryExhausted` with clear logging for Airflow admins.

## See also

- [How-To Guides](how-to-guides.md) — run modes, watermarks, multi-org pattern, debugging
- [Extending Reference](extending-reference.md) — asset attributes, token managers, contracts
- [Tutorial: Build Your First Asset](tutorial-first-asset.md) — step-by-step walkthrough
- [Testing Guide](testing.md) — test structure, fixtures, patterns
