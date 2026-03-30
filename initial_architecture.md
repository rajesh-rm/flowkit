# Data Assets Package — Architecture Document

> **Note**: This is the original design specification. The implementation follows it closely
> but differs in some details (e.g., some planned attributes like `date_format` and `earliest_date`
> were not implemented; the rate limiter uses a sliding-window instead of token bucket).
> For current, accurate documentation, see the files in `docs/`.

> **Purpose**: This document is the specification for building the `data_assets` Python package.
> It is designed to be consumed by a developer or AI coding agent implementing the package.
> It intentionally omits verbose code examples in favor of clear structural and behavioral specifications.

---

## 1. Overview

`data_assets` is a Python package that encapsulates all ETL logic for data assets. Apache Airflow imports this package and delegates all extraction, loading, and transformation work to it. Each Airflow DAG runs exactly one asset.

### Core Principles

- **Atomic runs**: Every run writes to a temporary table. The main table is only modified upon successful completion and validation.
- **Resumable extraction**: Checkpoints persist per-run so retried tasks resume without re-fetching data.
- **Self-managing schemas**: The package owns all DDL — creating and evolving target tables from asset class definitions.
- **In-process rate limiting**: Each DAG enforces outbound API call rate limits at runtime (no shared external state).
- **Parallel extraction**: Thread-based parallelism for page fan-out and entity fan-out patterns, with shared rate limiting and checkpoint-based resumption.
- **Token management**: A pluggable token manager per source handles credential lifecycle, including mid-run token rotation.
- **Separation of concerns**: Airflow knows *when* and *whether* to run. The package knows *how*.

---

## 2. Design Decisions

| Decision | Choice |
|---|---|
| Asset definition style | Python classes inheriting from base `Asset` |
| Load strategies | Full reset, forward fill, backward backfill, DB transform |
| Rate limiting | In-process per-DAG; max calls/sec defined on asset, overridable from DAG |
| Failure/retry model | Temp table per run + checkpoint tracking; Airflow retries resume from checkpoint |
| Schema management | Package auto-creates and manages tables from asset column definitions |
| Postgres layout | Single DB now; target schema configurable per asset; modular for multi-DB future |
| Observability | Python logging to stdout + run_history metrics table in `data_ops` schema |
| DB layer | SQLAlchemy ORM (metadata tables) + Core (data operations) |
| In-memory format | pandas DataFrames |
| Supported API sources | ServiceNow, SonarQube, GitHub, Jira (out of the box) |
| Parallel extraction | In-process thread pool; page-parallel and entity-parallel patterns |
| Notifications | Not in scope — Airflow handles alerting |

---

## 3. Postgres Schema Layout

All schemas live in a single Postgres database. Each asset declares its own `target_schema`, making it straightforward to route assets to different schemas or databases in the future.

| Schema | Purpose |
|---|---|
| `raw` | Default landing zone for API-sourced assets |
| `mart` | Transformed / derived assets (Postgres-to-Postgres) |
| `temp_store` | One unlogged temp table per active run. Dropped immediately after successful promotion. |
| `data_ops` | Operational metadata: locks, run history, checkpoints, coverage tracking, asset registry |

---

## 4. `data_ops` Metadata Tables

### 4.1 `data_ops.run_locks`

Mutex table preventing concurrent runs of the same asset. A row exists only while a run is active.

| Column | Type | Notes |
|---|---|---|
| asset_name | TEXT | PK. Only one row per asset can exist. |
| run_id | UUID | Identifies this run attempt. |
| locked_at | TIMESTAMPTZ | When the lock was acquired. |
| locked_by | TEXT | Airflow worker identifier. |

**Behavior**: INSERT at run start (fails if row exists = another run is active). DELETE on completion (success or failure). A stale lock detection mechanism should exist — if `locked_at` is older than a configurable threshold (e.g., 6 hours), the lock is considered stale and can be overridden with a warning log.

### 4.2 `data_ops.run_history`

Append-only log of every completed run with metrics.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL | PK. |
| run_id | UUID | Matches the run_id from the lock. |
| asset_name | TEXT | |
| run_mode | TEXT | 'full', 'forward', 'backfill', 'transform' |
| status | TEXT | 'success', 'failed' |
| started_at | TIMESTAMPTZ | |
| completed_at | TIMESTAMPTZ | |
| rows_extracted | INTEGER | |
| rows_loaded | INTEGER | After promotion. |
| error_message | TEXT | NULL on success. |
| metadata | JSONB | Arbitrary context: date range, params, retry count. |
| airflow_run_id | TEXT | Links to Airflow DAG run. |

### 4.3 `data_ops.checkpoints`

Stores extraction progress so retried runs resume from the last successful batch. For parallel extraction, multiple checkpoint rows may exist per run — one per worker partition.

| Column | Type | Notes |
|---|---|---|
| id | SERIAL | PK. |
| run_id | UUID | FK to run_locks. |
| asset_name | TEXT | |
| worker_id | TEXT | Identifies the parallel partition (e.g., `"pages_51_100"`, `"entity_repo_xyz"`). Value is `"main"` for non-parallel assets. |
| checkpoint_type | TEXT | 'page', 'cursor', 'offset', 'date_window' |
| checkpoint_value | JSONB | State needed to resume (cursor, page number, date boundary, etc.) |
| rows_so_far | INTEGER | Rows written to temp table by this worker at this checkpoint. |
| status | TEXT | 'in_progress', 'completed', 'failed'. Used on retry to skip completed partitions. |
| updated_at | TIMESTAMPTZ | |

### 4.4 `data_ops.asset_registry`

Canonical list of assets known to the system. Auto-populated when assets are discovered.

| Column | Type | Notes |
|---|---|---|
| asset_name | TEXT | PK. |
| asset_type | TEXT | 'api', 'transform' |
| source_name | TEXT | e.g., 'servicenow', 'github', 'sonarqube', 'jira' |
| target_schema | TEXT | |
| target_table | TEXT | |
| load_strategy | TEXT | |
| registered_at | TIMESTAMPTZ | |
| last_success_at | TIMESTAMPTZ | Updated on successful completion. |
| config | JSONB | Serialized non-sensitive asset config. |

### 4.5 `data_ops.coverage_tracker`

Tracks the data time boundaries each asset has successfully loaded. This is what answers "where do I start?" for forward and backfill modes.

| Column | Type | Notes |
|---|---|---|
| asset_name | TEXT | PK. |
| forward_watermark | TIMESTAMPTZ | Most recent data timestamp successfully loaded. |
| backward_watermark | TIMESTAMPTZ | Oldest data timestamp successfully loaded. |
| updated_at | TIMESTAMPTZ | |

---

## 5. Package Structure

```
data_assets/
├── __init__.py                     # Package entry, exposes run_asset()
├── runner.py                       # Main orchestrator: run_asset(name, mode, **overrides)
│
├── core/
│   ├── __init__.py
│   ├── asset.py                    # Base Asset class
│   ├── api_asset.py                # APIAsset subclass (for API sources)
│   ├── transform_asset.py          # TransformAsset subclass (Postgres-to-Postgres)
│   ├── enums.py                    # RunMode, LoadStrategy, AssetType enums
│   ├── registry.py                 # Asset discovery and auto-registration
│   ├── run_context.py              # RunContext: immutable dataclass for run params
│   └── column.py                   # Column definition: name, pg_type, nullable, default
│
├── extract/
│   ├── __init__.py
│   ├── api_client.py               # HTTP client: rate limiting, token injection, error classification, retries
│   ├── rate_limiter.py             # In-process token-bucket rate limiter with jitter (thread-safe)
│   ├── pagination.py               # Pagination strategy helpers (cursor, offset, page_number, keyset)
│   ├── parallel.py                 # Thread pool executor for page-parallel and entity-parallel extraction
│   ├── flatten.py                  # flatten_record() and pick_fields() utilities for nested JSON
│   └── token_manager.py            # Pluggable credential manager with mid-run rotation support
│
├── load/
│   ├── __init__.py                 # Re-exports public functions from loader.py
│   └── loader.py                   # DDL, temp tables, and promotion (full_replace/upsert/append)
│
├── transform/
│   ├── __init__.py
│   └── db_transform.py             # Execute SQL/Python transforms on Postgres source tables
│
├── checkpoint/
│   ├── __init__.py
│   └── manager.py                  # Read/write/clear checkpoints in data_ops.checkpoints
│
├── observability/
│   ├── __init__.py
│   ├── logging.py                  # Configured stdlib logger, stdout output for Airflow
│   └── run_tracker.py              # Write to run_history, coverage tracking, run metrics
│
├── db/
│   ├── __init__.py
│   ├── engine.py                   # SQLAlchemy engine factory with connection pooling
│   └── models.py                   # ORM models for all data_ops tables
│
├── validation/
│   ├── __init__.py
│   └── validators.py               # Composable validators: row count, PK uniqueness, nulls, schema match
│
└── assets/                         # All asset definitions
    ├── __init__.py
    ├── servicenow/
    │   └── (asset classes)
    ├── github/
    │   └── (asset classes)
    ├── sonarqube/
    │   └── (asset classes)
    ├── jira/
    │   └── (asset classes)
    └── transforms/
        └── (transform asset classes)
```

---

## 6. Class Hierarchy

### 6.1 `Asset` (Base)

All assets inherit from this. Defines:

- **Identity**: `name` (unique string), `description`.
- **Target**: `target_schema` (default `"raw"`), `target_table`, `columns` (list of `Column` objects), `primary_key` (list of column names).
- **Behavior**: `default_run_mode` (enum), `load_strategy` (enum).
- **Hooks** (override in subclasses):
  - `transform(df) -> df` — post-extraction pandas transform. Default: identity (no-op).
  - `validate(df, context) -> ValidationResult` — post-transform validation. Default: row count > 0 and PK not null.

### 6.2 `APIAsset(Asset)`

For assets sourced from external APIs. Adds:

- **Source identity**: `source_name` (string, e.g., `"github"`), `base_url`.
- **Token management**: `token_manager_class` — reference to a `TokenManager` subclass that handles credential lifecycle for this source.
- **Rate limiting**: `rate_limit_per_second` (float) — maximum outbound API calls per second. Enforced in-process. Overridable from DAG kwargs.
- **Pagination**: `pagination_config` — strategy (`"cursor"`, `"offset"`, `"page_number"`, `"date_window"`, `"none"`), page size, cursor field path, etc.
- **Parallel extraction**: `parallel_mode` (enum: `NONE`, `PAGE_PARALLEL`, `ENTITY_PARALLEL`), `max_workers` (int, default 1). See Section 9 for detailed behavior of each mode.
  - For `PAGE_PARALLEL`: asset also declares `total_pages_field` — the JSON path in the first API response that indicates total pages/count.
  - For `ENTITY_PARALLEL`: asset declares `parent_asset_name` — the asset whose primary keys are used as the list of entities to fan out across, and `build_entity_request(entity_key, context, checkpoint) -> RequestSpec` as an additional required override.
- **Incremental support**: `date_column` (column used for coverage tracking), `api_date_param` (query param name for date filtering), `date_format`.
- **Required overrides**:
  - `build_request(context, checkpoint) -> RequestSpec` — constructs the HTTP request for the current extraction window, incorporating checkpoint state for resumption.
  - `parse_response(response) -> (DataFrame, PaginationState)` — parses API response into rows and pagination continuation state.

### 6.3 `TransformAsset(Asset)`

For assets derived from existing Postgres tables. Adds:

- **Source**: `source_schema` (default `"raw"`), `source_tables` (list of dependency table names).
- **Required override**: `query(context) -> str` — returns a SQL SELECT producing the output rows.
- **Optional override**: `transform(df) -> df` — Python post-processing after SQL execution.
- Default `run_mode` is `FULL`. Default `target_schema` is `"mart"`.

### 6.4 Supporting Types

- **`Column`**: Dataclass with `name`, `pg_type` (Postgres type string), `nullable` (bool), `default` (optional).
- **`RunContext`**: Frozen dataclass with `run_id`, `mode`, `asset_name`, `start_date`, `end_date`, `params` (dict of overrides from Airflow).
- **`RunMode`**: Enum — `FULL`, `FORWARD`, `BACKFILL`, `TRANSFORM`.
- **`LoadStrategy`**: Enum — `FULL_REPLACE`, `UPSERT`, `APPEND`.
- **`ParallelMode`**: Enum — `NONE` (sequential, default), `PAGE_PARALLEL` (fan out pages across threads), `ENTITY_PARALLEL` (fan out parent entities across threads).
- **`PaginationConfig`**: Dataclass with `strategy`, `page_size`, `cursor_field`, `total_field`.
- **`RequestSpec`**: Dataclass with `method`, `url`, `params`, `headers`, `body`.
- **`PaginationState`**: Dataclass with `has_more`, `cursor`, `next_offset`, etc.
- **`ValidationResult`**: Dataclass with `passed` (bool), `failures` (list of strings).

---

## 7. Token Manager

The `TokenManager` is a pluggable class responsible for providing valid credentials to the API client. It is called by `api_client` before each request (or batch of requests) to get the current token. Implementations handle their own refresh logic.

### Base Interface

- `get_token() -> str` — returns a valid token/credential string. Implementations must handle caching and refresh internally.
- `get_auth_header() -> dict` — returns the appropriate HTTP header(s) (e.g., `{"Authorization": "Bearer <token>"}`).

### Required Implementations

| Source | Class | Behavior |
|---|---|---|
| GitHub | `GitHubAppTokenManager` | Generates a GitHub App installation token (1-hour validity). Tracks expiry internally. When token is within ~5 minutes of expiry, proactively generates a new one. Long-running extractions seamlessly pick up the refreshed token. |
| ServiceNow | `ServiceNowTokenManager` | Supports OAuth2 client_credentials flow. Obtains access token, refreshes on expiry. Also supports basic auth fallback. |
| SonarQube | `SonarQubeTokenManager` | Static API token. `get_token()` simply returns the configured value. Supports both token auth and basic auth with token-as-password. |
| Jira | `JiraTokenManager` | Supports Jira Cloud OAuth2 (3LO) or API token auth. For Jira Cloud: static API token with email-based basic auth. For Jira Data Center: personal access token as Bearer. |

### Credential Resolution

Token managers receive their initial secrets (client IDs, private keys, static tokens) via a `CredentialResolver` interface that checks, in order:
1. Airflow Connections (by a configurable key)
2. Environment variables
3. `.env` file (local development only)

The token manager does not store raw secrets — it resolves them at initialization time from the resolver.

---

## 8. Rate Limiter

The rate limiter is an **in-process** token bucket. It runs inside the same Python process as the DAG task. No shared state across workers — each DAG runs one asset, and the limiter constrains that single asset's outbound call rate.

### Behavior

- Configured via `rate_limit_per_second` on the asset class (float, e.g., `10.0` for 10 calls/sec, `0.5` for 1 call every 2 seconds).
- Overridable from DAG kwargs so the same asset can be run at different rates (e.g., slower during business hours).
- The `api_client` calls `limiter.acquire()` before every HTTP request. If the bucket is empty, it sleeps until a token is available.
- Also respects HTTP `429` responses: if a `Retry-After` header is present, the limiter pauses for that duration regardless of token availability.

### Implementation

Simple in-memory token bucket with `max_tokens` = `rate_limit_per_second` and refill rate = `rate_limit_per_second` tokens/sec. No database tables, no locks against external state. This is intentionally simple — concurrency control across DAGs is an Airflow concern (pools, concurrency limits), not this package's.

**Thread safety**: The rate limiter must be thread-safe (`threading.Lock` around token accounting) because parallel extraction uses multiple threads sharing a single limiter instance. This ensures that an asset with `rate_limit_per_second=10` and `max_workers=4` still makes at most 10 calls/sec total, not 10 per thread.

---

## 9. Run Lifecycle

When Airflow calls `run_asset("my_asset", mode="forward", **overrides)`:

### Phase 1: Initialize
1. Look up asset class from registry by name.
2. Acquire run lock (INSERT into `data_ops.run_locks`). If lock exists and is not stale, raise error (another run is active).
3. Generate `run_id` (UUID).
4. Read `coverage_tracker` to determine date boundaries for this mode.
5. Check for existing checkpoint (indicates a retry — previous attempt failed).
6. Build `RunContext` with computed date window, mode, and any DAG overrides.

### Phase 2: Extract (API assets only)
7. Initialize token manager and rate limiter (applying any overrides from DAG kwargs).
8. Create temp table in `temp_store` schema (or reuse existing one if checkpoint found).
9. **Extract** — behavior depends on the asset's `parallel_mode`:

**Sequential mode (`NONE`)**: Standard extract loop — build request, call API, parse response, append to temp table, update checkpoint, repeat until pagination exhausted.

**Page-parallel mode (`PAGE_PARALLEL`)**: See Section 9A below.

**Entity-parallel mode (`ENTITY_PARALLEL`)**: See Section 9B below.

In all modes, every HTTP request goes through the shared rate limiter and token manager. The rate limiter is thread-safe; the token manager returns the same (or refreshed) token to all threads.

### Phase 2 (alternate): Transform assets
7. Execute `asset.query(context)` against source tables.
8. Load result into DataFrame.
9. Write to temp table in `temp_store`.

### Phase 3: Transform & Validate
10. Read complete temp table into DataFrame.
11. Run `asset.transform(df)` — apply any Python transformations.
12. Write transformed data back to temp table (if transform modified anything).
13. Run `asset.validate(df, context)`. If validation fails: mark run as failed, preserve temp table for debugging, release lock, raise error.

### Phase 4: Promote
14. Ensure main target table exists (schema manager creates if missing, adds new columns if asset definition expanded).
15. Promote temp table to main table using the asset's `LoadStrategy`:
    - `FULL_REPLACE`: Truncate main + INSERT...SELECT from temp, in one transaction.
    - `UPSERT`: INSERT...ON CONFLICT DO UPDATE from temp.
    - `APPEND`: INSERT...SELECT from temp.
16. All promotion SQL runs in a single transaction. Failure = rollback, main table untouched.

### Phase 5: Finalize
17. Update `coverage_tracker` with new forward/backward watermark.
18. Write completed run to `run_history` (success or failure, with metrics).
19. Clear checkpoint for this asset.
20. **Drop the temp table** (default behavior on success).
21. Release run lock (DELETE from `run_locks`).

### On Failure (at any phase)
- Write failure record to `run_history`.
- Release run lock.
- Checkpoint is **preserved** (enables retry resumption).
- Temp table is **preserved** (enables debugging).
- Re-raise the exception so Airflow can handle retry logic.

### 9A. Page-Parallel Extraction

Used when a single paginated endpoint has many pages and the total is discoverable from the first response. Typical use cases: listing all GitHub repos in an org, all SonarQube projects, all ServiceNow records of a type, all Jira issues in a project.

**Sequence**:

1. **Discovery call**: The runner makes a single initial API request (page 1). The response includes the total number of pages or total record count (via the asset's `total_pages_field`). This first page of data is also captured.
2. **Partition**: Divide the remaining pages (2 through N) into `max_workers` roughly equal ranges. Each range becomes a worker partition (e.g., worker 0 gets pages 2–26, worker 1 gets pages 27–51, etc.).
3. **Fan out**: Submit each partition to a thread pool (`concurrent.futures.ThreadPoolExecutor` with `max_workers` threads). Each worker:
   - Iterates through its assigned page range sequentially.
   - Each request goes through the shared rate limiter (thread-safe) and token manager.
   - Appends each page's parsed DataFrame to the shared temp table (writes are serialized at the DB level).
   - Updates its own checkpoint row in `data_ops.checkpoints` (identified by `worker_id`, e.g., `"pages_27_51"`), with status `"in_progress"`.
   - On completing its partition, sets checkpoint status to `"completed"`.
4. **Collect**: The runner waits for all workers to finish. If all succeed, proceed to Phase 3.

**On partial failure**: If one worker fails, the runner cancels remaining workers, saves all checkpoint states, and raises the error. On retry, the runner reads all checkpoint rows for this run: workers marked `"completed"` are skipped, workers marked `"in_progress"` resume from their last checkpoint position, and workers not yet started begin from scratch.

**Thread safety considerations**: The rate limiter's `acquire()` must be thread-safe (use `threading.Lock`). The token manager's `get_token()` must be thread-safe (use a lock around refresh logic). Temp table writes use separate DB connections from the pool. Checkpoint updates use per-worker rows to avoid contention.

### 9B. Entity-Parallel Extraction

Used when a child/secondary resource must be fetched for each known parent entity. Typical use cases: pulling all workflow runs for each GitHub repo, all issues for each Jira project, all incidents for each ServiceNow CMDB CI, all code smells for each SonarQube project.

**Prerequisites**: The parent asset must have already been loaded into its main table (e.g., `github_repos` must exist in `raw` before `github_workflow_runs` can run in entity-parallel mode).

**Sequence**:

1. **Load entity list**: The runner queries the parent asset's main table (identified by `parent_asset_name`) to get the list of parent entity keys (the parent table's primary key values). For example, all repo full names from `raw.github_repos`.
2. **Partition**: Divide the entity list into `max_workers` groups. Each group becomes a worker partition.
3. **Fan out**: Submit each group to a thread pool. Each worker:
   - Iterates through its assigned entities.
   - For each entity, calls `asset.build_entity_request(entity_key, context, checkpoint)` to construct the request.
   - Paginates through all pages for that entity (sequentially within the worker).
   - Appends parsed data to the shared temp table.
   - Updates its checkpoint row (identified by `worker_id`, e.g., `"entity_repo_xyz"`) after completing each entity, tracking which entities are done and the pagination state within the current entity.
   - On completing all assigned entities, sets checkpoint status to `"completed"`.
4. **Collect**: Same as page-parallel — wait for all workers, then proceed to Phase 3.

**On partial failure**: Same behavior as page-parallel. On retry, completed entities (tracked in checkpoint) are skipped. The worker resumes from the entity it was processing when it failed, at the page it was on.

**Entity-parallel assets** define `build_entity_request()` instead of (or in addition to) `build_request()`. The `parse_response()` method is shared — it handles the same response format regardless of which entity the request was for.

**Rate limiting with entity-parallel**: The rate limiter is shared across all threads. If the asset defines `rate_limit_per_second=10` and `max_workers=4`, all 4 workers collectively cannot exceed 10 calls/sec. This prevents multiplying the rate by the thread count.

---

## 10. Date Window Computation by Mode

| Mode | `start_date` | `end_date` | Typical strategy |
|---|---|---|---|
| `FULL` | None (no filter) | None | Fetch everything. `FULL_REPLACE` promotion. |
| `FORWARD` | `coverage_tracker.forward_watermark` | `now()` | Fetch new data since last run. `UPSERT` or `APPEND`. |
| `BACKFILL` | API's earliest or configured floor | `coverage_tracker.backward_watermark` | Fetch progressively older data. `UPSERT`. |
| `TRANSFORM` | Derived from source asset coverage or run full | Same | Depends on underlying data availability. |

**Backfill stop condition**: Extraction stops when the API returns an empty response for a date window, or when a configured `earliest_date` floor is reached (per-asset property).

---

## 11. Retry & Checkpoint Behavior

When Airflow retries a failed task, `run_asset()` is called again with the same logical parameters.

**Sequential assets**:
1. The runner finds an existing checkpoint for this asset (where `worker_id = "main"`).
2. It reuses the existing temp table in `temp_store`.
3. It reads the checkpoint to determine where extraction stopped.
4. The extract loop resumes from that exact point.

**Parallel assets (page-parallel or entity-parallel)**:
1. The runner finds all checkpoint rows for this asset's run.
2. It reuses the existing temp table.
3. Workers whose checkpoint status is `"completed"` are skipped entirely.
4. Workers whose checkpoint status is `"in_progress"` resume from their last saved position.
5. Workers with no checkpoint row are started from scratch.

**Result**: Zero wasted API calls on retry. Only incomplete work is re-attempted.

---

## 12. Schema Manager

Manages DDL for both temp tables and main target tables based on asset `columns` definitions.

| Scenario | Action |
|---|---|
| Target table doesn't exist | CREATE TABLE with all columns and PK constraint |
| Table exists, schema matches | No-op |
| Asset defines new columns not in table | ALTER TABLE ADD COLUMN (additive only) |
| Asset removed columns from definition | Log warning, do NOT drop columns (data safety) |
| Column type changed | Log error, refuse to proceed (requires manual migration) |

**Temp tables**: Created as `UNLOGGED` tables in `temp_store` schema for write performance. Named as `{asset_name}_{run_id_short}`. Dropped after successful promotion.

---

## 13. Validation Framework

Assets can override `validate(df, context)` or rely on the default, which checks:
- Row count > 0
- Primary key columns contain no nulls

The `validators` module provides composable building blocks: row count thresholds, full-null column detection, PK uniqueness checks, schema match verification, and custom callable validators.

Validation failure halts the run before promotion. The temp table is preserved for inspection. The failure reason is recorded in `run_history`.

---

## 14. Error Handling

| Error Type | Handling |
|---|---|
| HTTP 429 (rate limit) | Respect `Retry-After` header or backoff. Transparent to the run — does not count as failure. |
| HTTP 5xx (server error) | Retry with exponential backoff (configurable max retries per request, default 3). After exhaustion: save checkpoint, raise to Airflow. |
| HTTP 4xx (client error, not 429) | Fail immediately. Log full request context. Indicates a bug in asset definition. |
| Connection error / timeout | Retry with backoff (same as 5xx). |
| Token manager error | Retry token acquisition with backoff. If persistent: save checkpoint, raise. |
| Parallel worker failure | Cancel remaining workers. Each worker's checkpoint is preserved with its current status. On retry, completed workers are skipped; failed/in-progress workers resume. |
| Transform error | Fail, preserve temp table. |
| Validation failure | Fail, preserve temp table, log which checks failed. |
| Promotion failure | Fail, preserve temp table. Transaction rollback protects main table. |

---

## 15. DAG Interface

### Entry Point

The package exposes a single function:

`run_asset(asset_name: str, run_mode: str, **overrides) -> dict`

- `asset_name`: Matches the `name` attribute of an asset class.
- `run_mode`: One of `"full"`, `"forward"`, `"backfill"`, `"transform"`.
- `**overrides`: Optional runtime overrides, including:
  - `rate_limit_per_second`: Override the asset's default rate limit.
  - `max_workers`: Override the asset's default parallelism degree.
  - `start_date` / `end_date`: Override computed date windows.
  - Any other params passed through to `RunContext.params`.
- **Returns**: A dict with run metrics (`run_id`, `rows_extracted`, `rows_loaded`, `duration_seconds`, `status`).

### DAG Patterns

**Simple DAG**: A single `PythonOperator` calling `run_asset` with the asset name and mode.

**Backfill DAG**: Same structure, `run_mode="backfill"`, typically `schedule_interval=None` (manually triggered).

**Transform DAG with dependency**: Uses Airflow's `ExternalTaskSensor` to wait for upstream asset DAGs, then calls `run_asset` for the transform asset.

**DAG factory** (optional convenience): A helper function `create_dag(asset_name, schedule, run_mode, ...)` that generates a complete Airflow DAG object with sensible defaults, for assets that don't need custom task logic.

Each DAG should set `max_active_runs=1` to prevent concurrent runs of the same asset (complementing the `run_locks` mechanism).

---

## 16. Connection & Configuration

### Database Connection

- SQLAlchemy engine created once per worker process, reused via connection pooling (`QueuePool`).
- Connection string resolved from Airflow Connections or environment variables.
- `pool_pre_ping=True` to handle stale connections.

### Credential Resolution Order

1. Airflow Connections (by configurable key)
2. Environment variables
3. `.env` file (local development only)

Token managers receive initial secrets from this resolver at initialization. The package never stores raw credentials.

---

## 17. Future Extension Points

These are not built now but the architecture explicitly supports them:

| Extension | How the design accommodates it |
|---|---|
| Multi-database targets | Each asset declares `target_schema`; add optional `target_connection` with a separate engine |
| New API sources | Add a new `TokenManager` subclass + asset classes in `assets/` |
| SCD Type 2 | New `LoadStrategy.SCD2` in promoter; asset defines `valid_from`/`valid_to` columns |
| Data lineage | `TransformAsset.source_tables` declares dependencies; exportable as a graph |

---

## 18. Build Order

Phases for implementing the package. Each phase should be buildable and testable independently before moving to the next.

### Phase 1: Foundation
**Build**: `core/enums.py`, `core/column.py`, `core/run_context.py`, `db/models.py`, `db/engine.py`

What this gives you: All data types, enums (including RunMode, LoadStrategy, ParallelMode), the RunContext dataclass, SQLAlchemy ORM models for every `data_ops` table, and the engine factory.

**Test**: ORM models can create all `data_ops` tables against a test Postgres instance. Engine connects and pools correctly.

### Phase 2: Base Classes & Registry
**Build**: `core/asset.py`, `core/api_asset.py`, `core/transform_asset.py`, `core/registry.py`

What this gives you: The full class hierarchy. Registry can discover and register asset classes from the `assets/` directory.

**Test**: Define a minimal stub asset, verify it registers, verify its properties are accessible.

### Phase 3: Checkpoint & Locking
**Build**: `checkpoint/manager.py`, `observability/metrics.py`, `observability/logging.py`

What this gives you: Read/write/clear checkpoints. Run lock acquire/release. Run history recording. Structured stdout logging.

**Test**: Lock acquisition, stale lock detection, checkpoint CRUD, run history insertion.

### Phase 4: Schema Management & Temp Tables
**Build**: `load/schema_manager.py`, `load/temp_table.py`

What this gives you: DDL generation from Column definitions. Temp table create/drop. Main table create-if-missing and additive column migration.

**Test**: Create tables from column lists, verify schema diffing, verify temp table lifecycle.

### Phase 5: Extraction Pipeline
**Build**: `extract/token_manager.py`, `extract/rate_limiter.py`, `extract/pagination.py`, `extract/api_client.py`, `extract/parallel.py`

What this gives you: Token managers for GitHub, ServiceNow, SonarQube, Jira. In-process thread-safe rate limiter. Pagination strategies. HTTP client that ties token management + rate limiting + request execution together. Parallel executor supporting both page-parallel and entity-parallel fan-out patterns.

**Test**: Token manager refresh logic (mock HTTP). Rate limiter timing accuracy and thread safety under concurrent access. Pagination state machines. API client integration with mocked endpoints. Parallel executor: verify page-parallel partitioning, entity-parallel fan-out, checkpoint-based resumption of partially completed parallel runs, and that rate limits are respected globally across threads.

### Phase 6: Load & Promotion
**Build**: `load/strategies.py`, `load/promoter.py`, `validation/validators.py`

What this gives you: Full replace, upsert, append promotion strategies. Composable validators.

**Test**: Each promotion strategy against real Postgres temp+main tables. Validator pass/fail scenarios.

### Phase 7: Transform Support
**Build**: `transform/db_transform.py`

What this gives you: Execute SQL queries against source tables, load results into temp table, apply optional Python transforms.

**Test**: Simple SQL transform end-to-end with test tables.

### Phase 8: Runner (Orchestrator)
**Build**: `runner.py`, `__init__.py`

What this gives you: The complete `run_asset()` function that ties all phases together. Package-level entry point.

**Test**: Full end-to-end run with a mock API asset. Full end-to-end run with a transform asset. Retry-with-checkpoint scenario. Lock contention scenario.

### Phase 9: Asset Definitions
**Build**: Asset classes in `assets/servicenow/`, `assets/github/`, `assets/sonarqube/`, `assets/jira/`, `assets/transforms/`

What this gives you: Real, production-ready asset definitions for all four API sources. Includes both root-level assets (sequential or page-parallel) and child-level assets (entity-parallel).

**Test**: Each asset against its real API (with rate limits set conservatively). Verify page-parallel assets correctly discover total pages and fan out. Verify entity-parallel assets correctly read parent keys and fan out.

### Phase 10: Airflow Integration
**Build**: Optional DAG factory helper. Example DAG files (not part of the package, but provided as reference templates).

**Test**: DAGs parse correctly in Airflow. End-to-end run via Airflow triggering `run_asset()`.
