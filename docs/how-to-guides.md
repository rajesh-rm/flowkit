# How-To Guides

Task-oriented guides for common operations. Each section is self-contained — find the task you need and follow the steps.

---

## Table of Contents

1. [How to choose a run mode](#how-to-choose-a-run-mode)
2. [How watermarks work](#how-watermarks-work)
3. [How to run against a test slice of data](#how-to-run-against-a-test-slice-of-data)
4. [How to debug a failed run](#how-to-debug-a-failed-run)
5. [How to reset local state](#how-to-reset-local-state)
6. [How to set up multi-org runs](#how-to-set-up-multi-org-runs)
7. [How to pass secrets from Airflow](#how-to-pass-secrets-from-airflow)
8. [How to monitor runs](#how-to-monitor-runs)
9. [Adding endpoints to existing sources](#adding-endpoints-to-existing-sources)

---

## How to choose a run mode

| Mode | When to use |
|------|-------------|
| `full` | Initial load or periodic full refresh |
| `forward` | Incremental — fetch new data since last run |
| `backfill` | Fill in historical data going backwards |
| `transform` | Run SQL transforms (database-to-database) |

Use this decision tree:

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

## How watermarks work

The framework tracks **what time range each asset has covered** in the `data_ops.coverage_tracker` table. Each asset has a `forward_watermark` (newest data loaded) and `backward_watermark` (oldest data loaded).

When you run in `forward` mode:
- `start_date` = the asset's `forward_watermark` (where the last run left off)
- `end_date` = now

When you run in `backfill` mode:
- `start_date` = None (beginning of time)
- `end_date` = the asset's `backward_watermark` (where the last backfill stopped)

In `full` mode, both are None — fetch everything.

**Important**: The framework computes this date window and passes it in `context.start_date` / `context.end_date`, but the **asset's `build_request()` must actually use it** to filter API calls. The framework does not automatically append date filters. If an asset's API has no date filter (e.g., GitHub branches), the asset uses `full` mode and re-fetches everything each run.

---

## How to run against a test slice of data

Assets like `github_prs` or `servicenow_incidents` can take hours to run in full against a real org. Use `max_pages` and `max_entities` to fetch a small slice of data and validate the flow without waiting:

```python
from data_assets import run_asset

# Fetch at most 3 pages — then stop.  dry_run skips the DB write.
result = run_asset("github_prs", run_mode="full", max_pages=3, dry_run=True)

# For entity-parallel assets with many parent entities (e.g., 52K repos):
# max_entities limits how many repos to process, max_pages limits pages per repo.
result = run_asset("github_commits", max_entities=10, max_pages=1, dry_run=True)
```

`max_pages` works across all extraction modes (sequential, page-parallel, entity-parallel, ServiceNow, and SonarQube). `max_entities` only applies to entity-parallel assets — a warning is logged if set on other modes. See [configuration.md](configuration.md#runtime-overrides) for the full per-mode behavior reference.

### max_pages behavior per extraction mode

| Mode | What `max_pages=N` means |
|------|--------------------------|
| Sequential | Stop after N API calls |
| Page-parallel (e.g., Jira issues) | Fetch N pages total across all workers |
| Entity-parallel (e.g., GitHub PRs) | Each entity (repo) gets at most N pages |
| ServiceNow | Stop after N batches of 1,000 records |
| SonarQube Projects | Cap each pagination shard at N pages |

### max_entities

`max_entities` limits how many parent entities the extractor processes in entity-parallel mode. This is useful when the parent table has thousands of entities (e.g., 52K repos) and you want a quick smoke test without waiting for all of them.

The slice is applied **after** `filter_entity_keys()`, so the limited set only includes entities from the correct org/partition.

> **Do not use `max_pages` or `max_entities` in production.** Partial data will overwrite the full dataset with `FULL_REPLACE`, and can leave `UPSERT` tables incomplete. Always pair with `dry_run=True` to prevent any writes to the target table.

---

## How to debug a failed run

### Setup and environment errors

| Symptom | Likely cause |
|---------|-------------|
| `RuntimeError: No database connection found...` | `DATABASE_URL` not set. Export it or add to `.env` file in the repo root |
| `ConnectionRefusedError` / `OperationalError` | Database not running or wrong `DATABASE_URL`. Verify the connection from Python (see [tutorial-dev-setup.md](tutorial-dev-setup.md#6-set-up-the-database)). |
| `RuntimeError: GitHubAppTokenManager requires GITHUB_APP_ID...` | Missing GitHub env vars. Set all four: `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY`, `GITHUB_INSTALLATION_ID`, `GITHUB_ORGS` |
| `RuntimeError: ServiceNowTokenManager requires SERVICENOW_INSTANCE` | Missing ServiceNow env vars. Set `SERVICENOW_INSTANCE`, `SERVICENOW_USERNAME`, `SERVICENOW_PASSWORD` |
| `RuntimeError: SonarQubeTokenManager requires SONARQUBE_TOKEN` | `SONARQUBE_TOKEN` env var not set |
| `RuntimeError: JiraTokenManager requires JIRA_PAT...` | Missing Jira creds. Set `JIRA_PAT` (Data Center) or `JIRA_EMAIL` + `JIRA_API_TOKEN` (Cloud) |
| `uv pip install` fails with SSL/certificate error | Corporate proxy doing TLS inspection — set `SSL_CERT_FILE` and `UV_NATIVE_TLS=true` (see [tutorial-dev-setup.md](tutorial-dev-setup.md#2c-custom-ca-certificates)) |
| `uv pip install` fails with timeout/connection refused | Proxy not configured — set `HTTPS_PROXY` (see [tutorial-dev-setup.md](tutorial-dev-setup.md#2a-httphttps-proxy)) |
| `pip install` downloads from wrong index | Internal mirror not configured — set `UV_INDEX_URL` or `PIP_INDEX_URL` (see [tutorial-dev-setup.md](tutorial-dev-setup.md#2b-internal-pypi-index-artifactory--nexus--devpi)) |
| `podman: command not found` or Docker socket errors | Container runtime not set up — see [tutorial-dev-setup.md](tutorial-dev-setup.md#3-container-runtime-podman) |
| Integration tests skip with `No Postgres available` | Podman socket not active. Run `systemctl --user start podman.socket` and export `DOCKER_HOST` |

### Runtime errors

| Symptom | Likely cause |
|---------|-------------|
| `KeyError: Asset 'xyz' not found in registry` | Typo in asset name, or missing `@register` decorator / `__init__.py` import. Run `data-assets list` to see registered names |
| `build_request` never called | Entity-parallel assets use `build_entity_request` instead |
| API returns errors | `base_url` is empty — make sure the source env var is set and read at runtime in `build_request` |
| Data missing from table | Column names in `parse_response` DataFrame don't match the asset's `columns` list |
| Duplicate rows | Check `primary_key` is set correctly, use `UPSERT` load strategy |
| `ValueError: Validation failed for 'X'` | Extracted data failed a validator (e.g., null primary keys, empty DataFrame). Check the error details |
| `TypeError: Asset 'X' has type Y, expected APIAsset...` | Custom asset class doesn't inherit from `APIAsset` or `TransformAsset` |
| `LockError: Asset 'X' is locked by run ...` | Previous run still active or crashed. Locks are released automatically on failure (including Ctrl+C). If a lock is stuck, it auto-clears after `stale_heartbeat_minutes` (default 20 min) or `max_run_hours` (default 5h) |
| `RuntimeError: Checkpoint rejected` | Another worker took over your run (stale-run takeover). Normal recovery — retry the task |
| `DatabaseRetryExhausted: ... after 3 attempts` | Database unreachable or overloaded. Check DB is running, verify `DATABASE_URL`. Adjust `DATA_ASSETS_DB_RETRY_ATTEMPTS` / `DATA_ASSETS_DB_RETRY_BASE_DELAY` if transient |
| `ValueError: Column 'X' has N value(s) exceeding max length` | API returned data longer than the asset's `column_max_lengths` limit. Check the asset class — increase the limit or investigate the source data. Stale checkpoints from the failed run are auto-cleared, so a retry will re-extract fresh data |
| `MissingKeyError: required column 'X' (API field '...') is absent from response record #N` | The API omitted a key that the asset expects. Either real schema drift (update the column list and `field_to_column` map in `parse_response()`) or a legitimately optional field (add the column to the asset's `optional_columns`). `null` values are NOT the trigger — only missing keys. See [extending-reference.md](extending-reference.md#run-failing-with-missingkeyerror) |
| Log line `Validation warning ... High null rate: Column 'X' has N% null rate` | Non-blocking — the run succeeds. Tune via the asset's `column_null_thresholds` (e.g., `{"closed_at": 1.0}` to silence) or raise `default_null_threshold`. Null rate is orthogonal to missing-key; if you need hard-fail, use `optional_columns` instead |
| `psycopg2.errors.NumericValueOutOfRange: integer out of range` | An ID column uses `Integer()` (32-bit, max 2.1B) but the source API returned a larger value. Change the column type to `BigInteger()`. All GitHub assets already use BigInteger for ID columns |
| Asset runs for hours locally | Use `max_pages=3, dry_run=True` to validate the flow against a small slice of real data. For entity-parallel assets with many entities (e.g., 52K repos), also use `max_entities=10` |
| `HTTPStatusError: 409 Conflict` on GitHub assets | Empty repos (no commits) return 409. This is handled automatically — the repo is skipped. If you see this error, your asset may not inherit from `GitHubRepoAsset` |
| `max_workers must be greater than 0` with 0 entities | `GITHUB_ORGS` case doesn't match repo names in the database (e.g., `td-universe` vs `TD-Universe`). Org filtering is case-insensitive, so check for typos or extra whitespace |

---

## How to reset local state

If you need to wipe test data:

```sql
-- Drop all asset data tables
DROP SCHEMA raw CASCADE; CREATE SCHEMA raw;
DROP SCHEMA mart CASCADE; CREATE SCHEMA mart;
DROP SCHEMA temp_store CASCADE; CREATE SCHEMA temp_store;

-- Clear operational metadata
TRUNCATE data_ops.run_locks, data_ops.run_history,
         data_ops.checkpoints, data_ops.asset_registry,
         data_ops.coverage_tracker;
```

The package re-creates schemas and metadata tables automatically on the next `run_asset()` call.

---

## How to set up multi-org runs

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

The example DAGs in `example_dags/flowkit_dags.py` already pass `partition_key=org_config["org"]` for all GitHub assets.

**Your asset doesn't need any code changes to support partition_key.** The framework handles lock and watermark scoping automatically via composite keys `(asset_name, partition_key)` on the operational tables.

**Entity-parallel scoping**: For assets like `github_pull_requests` that fan out by repository, the `GITHUB_ORGS` env var is set per-run by the DAG. The base class `filter_entity_keys()` in `assets/github/helpers.py` reads it to scope extraction to only that org's repos. The match is **case-insensitive** — `GITHUB_ORGS=td-universe` matches repos stored as `TD-Universe/repo`.

**DAG pattern**: Create one DAG per org. See `example_dags/flowkit_dags.py` for the production pattern, or `example_dags/dag_factory.py` for auto-generated DAGs from an Airflow Connection.

---

## How to pass secrets from Airflow

Instead of pre-setting env vars on workers, pass secrets explicitly from Airflow Connections via the `secrets` parameter:

```python
from airflow.sdk import BaseHook
from data_assets import run_asset

conn = BaseHook.get_connection("sonarqube")
run_asset("sonarqube_projects", secrets={
    "SONARQUBE_URL": f"https://{conn.host}",
    "SONARQUBE_TOKEN": conn.password,
})
```

`run_asset()` accepts a `secrets` dict — env var names to values. Secrets are injected into `os.environ` for the duration of the run and cleaned up after. Secrets are resolved at execution time on the worker, not at DAG parse time. With a secret backend (Vault, AWS SSM, GCP Secret Manager), values never touch Airflow's metadata DB.

### Airflow Connection setup (one-time)

```bash
airflow connections add sonarqube \
    --conn-type generic \
    --conn-host "sonar.company.com" \
    --conn-password "squ_your_token"

airflow connections add github_app \
    --conn-type generic \
    --conn-login "12345" \
    --conn-password "$(cat github-app-private-key.pem)" \
    --conn-extra '{"installation_id": "789", "orgs": "my-org"}'

airflow connections add jira \
    --conn-type generic \
    --conn-host "company.atlassian.net" \
    --conn-login "user@company.com" \
    --conn-password "jira-api-token"

airflow connections add servicenow \
    --conn-type generic \
    --conn-host "company.service-now.com" \
    --conn-login "etl_user" \
    --conn-password "password"
```

With a secret backend (Vault, AWS SSM), Airflow resolves these at runtime — values never touch the metadata DB.

### GitHub multi-org example

```python
from airflow.sdk import BaseHook
from data_assets import run_asset

def _run_github(**context):
    conn = BaseHook.get_connection("github_app")
    run_asset(
        "github_repos",
        run_mode="full",
        secrets={
            "GITHUB_APP_ID": conn.login,
            "GITHUB_PRIVATE_KEY": conn.password,
            "GITHUB_INSTALLATION_ID": conn.extra_dejson["installation_id"],
            "GITHUB_ORGS": conn.extra_dejson["orgs"],
        },
        airflow_run_id=context["run_id"],
    )
```

---

## How to monitor runs

- **Airflow UI**: Each asset is a separate DAG with tags by source
- **Run history**: Query `data_ops.run_history` for run metrics (includes error details and row counts)
- **Coverage**: Query `data_ops.coverage_tracker` to see watermarks and which date ranges have been loaded
- **Logs**: All output goes to stdout (captured by Airflow task logs)
- **Database retries**: Transient DB errors are retried automatically (up to `DATA_ASSETS_DB_RETRY_ATTEMPTS`, default 3). Each retry is logged at WARNING level. If retries exhaust, the run fails with `DatabaseRetryExhausted` — check logs for the underlying error (connection timeout, deadlock, etc.)
- **Data quality warnings**: The framework warns (non-blocking) if any string column contains values exceeding 10,000 characters. Assets with `column_max_lengths` defined will block promotion if limits are exceeded

---

## Adding endpoints to existing sources

Quick references for adding new assets to each built-in source. For the full tutorial on building a new asset from scratch, see [Tutorial: Build Your First Asset](tutorial-first-asset.md).

### Adding a SonarQube endpoint

Extend `SonarQubeAsset` (in `helpers.py`) which provides `token_manager_class`, `rate_limit_per_second`, and the `api_url` property. Pick the closest existing asset as your template:

| Pattern | Template | When to use |
|---------|----------|-------------|
| Unpaginated per-project | `branches.py` | API returns all data in one call per project |
| Paginated per-project | `analyses.py` | API uses `p`/`ps` pagination with `paging.total` |
| EAV response flattening | `measures.py` | One row per (project, branch, metric) — extracts `value` or `period.value` |
| Nested response flattening | `measures_history.py` | Metric-grouped time-series response to flatten into rows |

Key settings for SonarQube entity-parallel assets:
- Most assets: `parent_asset_name = "sonarqube_projects"` — fans out by project key, use `entity_key_column = "project_key"` if the response doesn't include it
- Measures assets: `parent_asset_name = "sonarqube_branches"` — fans out by (project, branch) pairs, use `entity_key_map` to inject fields not present in the API response (e.g., `{"name": "branch"}` when the response already contains `project_key`)
- Use `self.api_url` (not `os.environ.get(...)`) for the base URL
- Use `parse_paging()` from `helpers.py` for standard `paging` response extraction
- Metric constants: `DEFAULT_METRICS` (7 standard), `NEW_CODE_METRICS` (new code period), `ALL_METRICS` (combined — component endpoint), `HISTORY_METRICS` (22 metrics — search_history endpoint)
- SonarQube API docs: https://next.sonarqube.com/sonarqube/web_api

### Adding a ServiceNow table

Add a new `@register` class in `servicenow/tables.py` (all ServiceNow table assets live in one file). Inherit from `ServiceNowTableAsset` (in `servicenow/base.py`) and set `name`, `target_table`, `table_name`, `columns`, and `indexes`. Copy any existing class in `tables.py` as a starting template. Key notes:
- The base class handles all extraction, keyset pagination, and auth via pysnc — no `build_request()` or `parse_response()` needed
- Import the new class in `servicenow/__init__.py`
- ServiceNow query syntax: `^` = AND, `^OR` = OR
- Table API docs: https://docs.servicenow.com/bundle/latest/page/integrate/inbound-rest/concept/c_TableAPI.html

### Adding a GitHub endpoint

For repo-scoped endpoints, inherit from `GitHubRepoAsset` (in `assets/github/helpers.py`). Copy `github/branches.py` as a starting template — it's the simplest at ~40 lines.

Key settings (provided by `GitHubRepoAsset` base class):
- `token_manager_class`, `rate_limit_per_second`, `pagination_config`, `parallel_mode`, `max_workers`
- `parent_asset_name = "github_repos"`, `entity_key_column = "repo_full_name"`
- `filter_entity_keys()` (case-insensitive org filtering via `GITHUB_ORGS`)
- `classify_error()` — skips 409 (empty repos) in addition to the default 404 skip

You only need to define:
- `name`, `target_table`, `columns`, `primary_key`
- `build_entity_request()` — use `self._paginated_entity_request(entity_key, url_path, checkpoint)`
- `parse_response()` — use `self._parse_array_response(response, record_fn)` or `self._parse_wrapped_response(response, items_key, record_fn)`

For org-level endpoints (repos, members, runner groups), inherit from `GitHubOrgAsset` (in `assets/github/helpers.py`). It provides shared `build_request()` logic for org-scoped pagination. Subclasses set `org_endpoint` (e.g., `"/repos"`, `"/members"`) and optionally `org_request_params`, then implement `parse_response()`.
- **`since` param**: works on `/repos/{o}/{r}/commits` but NOT on `/pulls`
- GitHub REST API docs: https://docs.github.com/en/rest

### Adding a Jira endpoint

Inherit from `JiraAsset` (in `assets/jira/helpers.py`), which provides shared `source_name`, `token_manager_class`, `rate_limit_per_second`, and `get_jira_url()`. Copy `jira/issues.py` (JQL + entity-parallel) or `jira/projects.py` (sequential) as a starting template. Key settings:
- Pagination: `{"strategy": "offset", "page_size": 100}` with `startAt`/`maxResults`
- Use JQL for date filtering: `updated >= "{iso_date}"`
- For entity-parallel: set `parent_asset_name = "jira_projects"` (fans out by project key)
- Jira REST API v3 docs: https://developer.atlassian.com/cloud/jira/platform/rest/v3/

### When to use RestAsset vs APIAsset

| Use RestAsset when... | Use APIAsset when... |
|----------------------|---------------------|
| Standard REST: GET endpoint returns JSON with records array | API needs custom request logic (multi-org iteration, JQL construction) |
| Pagination is page_number, offset, or cursor | Pagination needs keyset or custom sort params |
| Field mapping is just renames | Response parsing needs nested extraction or type conversion |
| No incremental date filter needed (FULL_REPLACE) | Incremental needs sort-by-update or should_stop() |

**Example:** `sonarqube_projects` uses RestAsset with a custom `extract()` override (handles the 10k ES limit via query sharding). `sonarqube_issues` uses APIAsset (needs UPDATE_DATE sort).
