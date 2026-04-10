# User Guide

## Prerequisites

- Python 3.11+
- PostgreSQL 16+ or MariaDB 10.11+
- Apache Airflow 3.0+ (for DAG scheduling)

## Installation

```bash
pip install data-assets

# Or with dev dependencies:
pip install data-assets[dev]
```

## Quick Start

### 1. Set up the Database

Create a database and ensure the connecting user has `CREATE SCHEMA` privileges:

```sql
CREATE DATABASE data_assets;
```

The package auto-creates the required schemas (`data_ops`, `raw`, `mart`, `temp_store`) and all metadata tables on first run.

### 2. Configure credentials

Set environment variables for your database and sources:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/data_assets"
export SONARQUBE_URL="https://sonar.example.com"
export SONARQUBE_TOKEN="sqa_xxxxx"
```

See [configuration.md](configuration.md) for all source variables.

### 3. Run an asset

```python
from data_assets import run_asset

result = run_asset("sonarqube_projects", run_mode="full")
print(result)
# {'run_id': '...', 'rows_extracted': 42, 'rows_loaded': 42, 'duration_seconds': 3.2, 'status': 'success'}
```

### 4. Deploy with Airflow

Use the built-in CLI to generate DAG files for all registered assets:

```bash
# List all available assets
data-assets list

# Generate DAG files in your Airflow DAGs folder
data-assets sync --output-dir /opt/airflow/dags/data_assets/
```

This creates one DAG file per asset with sensible defaults (schedule, retries, tags). To customise schedules or use Airflow Connections for secrets, create a `dag_overrides.toml` file in the same directory.

For automated zero-touch updates (new package versions automatically generate new DAGs), see the [Airflow Deployment Guide](airflow-deployment.md).

**Passing secrets explicitly** (for remote workers or KubernetesExecutor):

```python
from airflow.sdk import BaseHook
from data_assets import run_asset

conn = BaseHook.get_connection("sonarqube")
run_asset("sonarqube_projects", secrets={
    "SONARQUBE_URL": f"https://{conn.host}",
    "SONARQUBE_TOKEN": conn.password,
})
```

See [configuration.md](configuration.md) for connection setup commands.

## Run Modes

| Mode | When to use |
|------|-------------|
| `full` | Initial load or periodic full refresh |
| `forward` | Incremental — fetch new data since last run |
| `backfill` | Fill in historical data going backwards |
| `transform` | Run SQL transforms (database-to-database) |

### Which mode should I use?

```
Is this the first time loading this asset?
  └─ YES → full
  └─ NO
      ├─ Do you need to catch up on new/updated data since last run?
      │     └─ YES → forward
      ├─ Do you need historical data from before your first load?
      │     └─ YES → backfill
      └─ Is this a derived table computed from other tables?
            └─ YES → transform
```

### How watermarks work

The framework tracks **what time range each asset has covered** in the `data_ops.coverage_tracker` table. Each asset has a `forward_watermark` (newest data loaded) and `backward_watermark` (oldest data loaded).

When you run in `forward` mode:
- `start_date` = the asset's `forward_watermark` (where the last run left off)
- `end_date` = now

When you run in `backfill` mode:
- `start_date` = None (beginning of time)
- `end_date` = the asset's `backward_watermark` (where the last backfill stopped)

In `full` mode, both are None — fetch everything.

**Important**: The framework computes this date window and passes it in `context.start_date` / `context.end_date`, but the **asset's `build_request()` must actually use it** to filter API calls. The framework does not automatically append date filters. If an asset's API has no date filter (e.g., GitHub branches), the asset uses `full` mode and re-fetches everything each run.

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

For the full lifecycle, see [architecture.md](architecture.md). For which assets support incremental mode, see [assets-catalog.md](assets-catalog.md).

---

## Multi-Org Runs (partition_key)

If you have multiple GitHub organizations (or any multi-tenant setup), use `partition_key` to run the same asset for each org **concurrently and independently**.

```python
run_asset(
    "github_repos",
    run_mode="full",
    partition_key="org-one",        # Scopes locks + watermarks to this org
    secrets={
        "GITHUB_APP_ID": "...",
        "GITHUB_INSTALLATION_ID": "111",
        "GITHUB_ORGS": "org-one",
    },
)
```

**What gets scoped per partition**: locks, watermarks, checkpoints, run history. Each org gets its own progress tracking — org-one's watermark doesn't affect org-two.

**What stays shared**: the target table. Both orgs write to `raw.github_repos` via UPSERT. Primary keys are org-scoped (e.g., `full_name = "org-one/repo-a"`), so there are no data conflicts.

**Without partition_key**, both orgs compete for the same lock and share a single watermark — org-two would block until org-one finishes, and incremental mode may over-fetch or under-fetch.

The example DAGs in `example_dags/flowkit_dags.py` already pass `partition_key=org_config["org"]` for all GitHub assets. See [extending.md](extending.md) for how to implement this pattern for new sources.

---

## Monitoring

- **Airflow UI**: Each asset is a separate DAG with tags by source
- **Run history**: Query `data_ops.run_history` for run metrics (includes error details and row counts)
- **Coverage**: Query `data_ops.coverage_tracker` to see watermarks and which date ranges have been loaded
- **Logs**: All output goes to stdout (captured by Airflow task logs)
- **Database retries**: Transient DB errors are retried automatically (up to `DATA_ASSETS_DB_RETRY_ATTEMPTS`, default 3). Each retry is logged at WARNING level. If retries exhaust, the run fails with `DatabaseRetryExhausted` — check logs for the underlying error (connection timeout, deadlock, etc.)
- **Data quality warnings**: The framework warns (non-blocking) if any string column contains values exceeding 10,000 characters. Assets with `column_max_lengths` defined will block promotion if limits are exceeded

## See also

- [Airflow Deployment](airflow-deployment.md) — deploy DAGs, zero-touch updates, systemd setup
- [Architecture](architecture.md) — how the ETL lifecycle works under the hood
- [Configuration](configuration.md) — all credential and runtime settings
- [Assets Catalog](assets-catalog.md) — every built-in asset and its design choices
- [Extending](extending.md) — how to add new data sources
