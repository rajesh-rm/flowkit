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
│ API     │ temp     │ locks     │ SQL      │ logging            │
│ client  │ tables   │ checkpts  │ transforms│ run_history       │
│ rate    │ schema   │           │          │ coverage           │
│ limiter │ promote  │           │          │                     │
│ tokens  │ strategy │           │          │                     │
│ parallel│          │           │          │                     │
├─────────┴──────────┴───────────┴──────────┴────────────────────┤
│                     core/ + db/                                  │
│  Asset classes, enums, types, registry, SQLAlchemy models        │
└─────────────────────────────────────────────────────────────────┘
```

## Run Lifecycle

When Airflow calls `run_asset("my_asset", mode="forward")`:

1. **Initialize** — Discover assets, acquire lock, read coverage watermarks, check for retry checkpoints
2. **Extract** — Fetch data via API client (sequential, page-parallel, or entity-parallel) into a temp table
3. **Transform & Validate** — Apply `asset.transform(df)`, run `asset.validate(df, context)`
4. **Promote** — Move from temp table to main table via FULL_REPLACE, UPSERT, or APPEND (single transaction)
5. **Finalize** — Update coverage tracker, record metrics, clear checkpoints, drop temp table, release lock

On failure: checkpoints and temp table are preserved for retry. Lock is released.

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
| Schema management | Auto-create, additive migration only | Safe evolution, no data loss |
| Rate limiting | In-process token bucket (thread-safe) | Simple, no external state |
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

### Sequential (NONE)
Default mode. The runner calls `build_request()` → API → `parse_response()` in a loop until `has_more=False`. Each iteration gets the latest checkpoint, so the asset controls the full URL and params.

### Page-Parallel
For endpoints where the total pages are discoverable from the first response. One discovery call fetches page 1 and reads the total. Remaining pages (2..N) are partitioned across `max_workers` threads. Each worker checkpoints independently. On retry, completed workers are skipped.

**Use when:** the API returns a total count/pages in the first response (e.g., SonarQube `paging.total`).

### Entity-Parallel
For child resources (e.g., PRs per repo, issues per project). The runner loads entity keys from a parent asset's table, partitions them across threads, and each worker paginates through all its assigned entities.

**Use when:** you need to fetch sub-resources for each item in a parent asset's table.

**Prerequisite:** `parent_asset_name` must reference an already-loaded asset.

Both parallel modes share a single rate limiter (thread-safe) ensuring global rate limits are respected.
