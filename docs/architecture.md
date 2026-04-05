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

On failure: lock is released, temp table is cleaned up, checkpoints are preserved for retry.

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

**Alternative path — `extract()` hook (e.g., ServiceNow/pysnc):**

Assets that override `extract()` bypass the diagram above. The runner calls `asset.extract(engine, temp_table, context)` directly, and the asset handles fetching and writing to the temp table using its own client:

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

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Load strategies | Full replace, upsert, append | Covers all ETL patterns |
| Failure model | Temp table + checkpoints | Zero wasted API calls on retry |
| Schema management | Auto-create, additive migration via SchemaContract enum (EVOLVE/FREEZE/DISCARD) | Safe evolution, no data loss |
| Rate limiting | In-process sliding-window counter (thread-safe) | Simple, no external state |
| Parallelism | Thread pool for page/entity fan-out | Shared rate limiter + token manager |
| DB layer | SQLAlchemy ORM (metadata) + Core (DDL) | Best of both worlds |
| In-memory format | pandas DataFrames | Standard, well-supported |

## Postgres Schema Layout

| Schema | Purpose |
|--------|---------|
| `raw` | Default landing zone for API-sourced assets |
| `mart` | Transformed / derived assets |
| `temp_store` | Unlogged temp tables (one per active run) |
| `data_ops` | Operational metadata: locks, history, checkpoints, registry, coverage |

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
- **Error handling**: `SkippedRequestError` (e.g., 404) skips the entity, doesn't kill the run
- **Thread pool**: `_run_workers()` caps pool size at `min(max_workers, work_units)` — no wasted threads

## Asset Definition: Four Paths

- **RestAsset** (declarative) — for standard REST APIs. Declare endpoint, pagination, field_map as class attributes. No `build_request()`/`parse_response()` needed. See `sonarqube/projects.py`.
- **APIAsset** (custom) — for APIs needing custom logic (JQL construction, keyset pagination, multi-org iteration). Override `parse_response()` and either `build_request()` (sequential) or `build_entity_request()` (entity-parallel).
- **GitHubRepoAsset** (shared base) — for GitHub repo-scoped entity-parallel assets. Provides token manager, pagination, org filtering, and response parsing helpers. See `assets/github/branches.py`.
- **ServiceNowTableAsset** (pysnc/extract hook) — for ServiceNow tables. Uses pysnc's GlideRecord client instead of httpx. Overrides `extract()` to bypass the API client pipeline entirely. Subclasses only set `name`, `target_table`, `table_name`, and `columns`. See `assets/servicenow/base.py`.

## Run Resilience

- **UUIDv7 run IDs** — timestamp-ordered, sortable. Each run gets a unique ID that sorts chronologically.
- **Stale-run takeover** — if a worker dies, the next retry detects the abandoned run (no heartbeat in 20 min OR exceeded 5 hour max), inherits its temp table and checkpoints, and resumes extraction.
- **Secrets injection** — `run_asset(secrets={...})` injects credentials as env vars for the run duration. Cleaned up in `finally` block. Airflow DAGs use this to pass secrets from Connections.
- **Entity-parallel unified checkpoint** — each checkpoint saves completed entities + current entity + pagination position, enabling exact mid-entity resume.
