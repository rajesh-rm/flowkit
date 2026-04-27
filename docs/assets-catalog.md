# Assets Catalog

This catalog documents every built-in asset. Use it as a reference when building new assets — find one with a similar pattern and use it as a starting point.

> **Sensitive data:** every registered asset currently declares `contains_sensitive_data=False`. The tokenization framework is in place but no asset is being tokenized in production yet. `github_user_details` (which carries `name`, `email`, `bio`, `location`, `company`) is the obvious first candidate when the tokenization endpoint is configured. See [extending-reference.md](extending-reference.md#sensitive-data-and-tokenization) for the declaration, validation rules, and behavior.

## SonarQube

**Authentication:** Static API token (Bearer) — `SonarQubeTokenManager`
**API docs:** https://next.sonarqube.com/sonarqube/web_api

### Project catalog

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `sonarqube_projects` | `raw.sonarqube_projects` | FULL_REPLACE | Custom `extract()` | page_number + sharded `q` param | `/api/components/search?qualifiers=TRK` |
| `sonarqube_project_details` | `raw.sonarqube_project_details` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | none (one call per project) | `/api/components/show` |

### Quality metrics

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `sonarqube_measures` | `raw.sonarqube_measures` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | none (one call per branch) | `/api/measures/component` |
| `sonarqube_measures_history` | `raw.sonarqube_measures_history` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/measures/search_history` |

### Issues and analysis

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `sonarqube_issues` | `raw.sonarqube_issues` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/issues/search` |
| `sonarqube_analyses` | `raw.sonarqube_analyses` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/project_analyses/search` |
| `sonarqube_analysis_events` | `raw.sonarqube_analysis_events` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/project_analyses/search` |
| `sonarqube_branches` | `raw.sonarqube_branches` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | none (all branches in one call) | `/api/project_branches/list` |

### Data dependency graph

Most entity-parallel assets depend directly on `sonarqube_projects`. The two measures assets depend on `sonarqube_branches` (which itself depends on `sonarqube_projects`) so they fan out by (project, branch) pairs.

```
sonarqube_projects
├── sonarqube_project_details
├── sonarqube_issues
├── sonarqube_analyses
├── sonarqube_analysis_events
└── sonarqube_branches
    ├── sonarqube_measures
    └── sonarqube_measures_history
```

Run `sonarqube_projects` first, then `sonarqube_branches` before either measures asset (or let the DAG factory handle ordering).

### Key design choices

**Shared base class.** All entity-parallel assets extend `SonarQubeAsset` (in `assets/sonarqube/helpers.py`), which sets `token_manager_class`, `source_name`, `target_schema`, and `rate_limit_per_second` (5 req/s). It also provides an `api_url` property that resolves `SONARQUBE_URL` from env vars. Four metric lists are defined in `helpers.py`: `DEFAULT_METRICS` (7 standard metrics), `NEW_CODE_METRICS` (`new_coverage`, `new_lines_to_cover`, `new_line_coverage`), `ALL_METRICS` (combined — used by `sonarqube_measures` for the component endpoint), and `HISTORY_METRICS` (22 metrics — used by `sonarqube_measures_history` for the search_history endpoint). The two endpoint-specific lists differ because the component and search_history APIs accept different metric key vocabularies (e.g., `duplicated_lines_density` vs `duplicated_lines`). `SonarQubeProjects` uses `RestAsset` instead and overrides `extract()` for the sharding logic.

**Why custom `extract()` for projects?** SonarQube's `/api/components/search` is backed by Elasticsearch, which caps results at 10,000. For instances with >9,900 projects, standard pagination fails. The custom extraction shards queries using the `q` parameter with 2-character alphanumeric prefixes, recursively extending for hot prefixes, and deduplicates by project key. Safety guards: max pages per shard, max recursion depth (4), >5% shortfall abort.

**Why a separate project_details asset?** `/api/components/search` only returns `key`, `name`, `qualifier`. Richer metadata (description, visibility, version, tags, analysis dates) requires a per-project call to `/api/components/show`.

**Why UPSERT for issues, analyses, and measures_history?** These assets run in FORWARD mode — they fetch only data created/updated since the last watermark. The same key may appear in overlapping date windows, so UPSERT by PK prevents duplicates.

**Why FULL_REPLACE for measures, branches, and analysis_events?** `sonarqube_measures` is a live snapshot — every run captures the complete current state of all project branches. Branches can be renamed or deleted between runs — FULL_REPLACE ensures stale branches are removed. Analysis events have no date column for incremental tracking, so a full refresh is the correct strategy.

**Why two assets for the same API (analyses + events)?** `/api/project_analyses/search` returns analyses with nested events. Rather than using JSONB or a custom multi-table `extract()` override, the events are extracted as a separate normalized asset. Both assets call the same endpoint independently — the API is hit twice per project, which is acceptable at 5 req/s with 3 workers. This keeps both assets in the standard entity-parallel pipeline with no custom extract logic.

**Multi-branch measures.** Both measures assets use `sonarqube_branches` as their parent, fanning out by `(project_key, branch)` pairs. Since the parent has a composite PK, entity keys are dicts like `{"project_key": "proj-1", "name": "main"}`. The `entity_key_map` attribute tells the framework which dict fields to inject as DataFrame columns after `parse_response()` — only fields **not already in the API response** need mapping. For `sonarqube_measures`, the response includes `project_key` via `component.key`, so only `branch` needs injection: `entity_key_map = {"name": "branch"}`. For `sonarqube_measures_history`, the response contains neither, so both are injected: `entity_key_map = {"project_key": "project_key", "name": "branch"}`. The `branch` API parameter requires SonarQube Developer Edition; Community Edition instances gracefully degrade to main-branch-only data (since `sonarqube_branches` returns only `main` for CE).

**New code metrics.** `sonarqube_measures` includes `new_coverage`, `new_lines_to_cover`, and `new_line_coverage` (from `ALL_METRICS`). The component API nests these values inside a `period` object (`{"period": {"value": "92.0"}}`) rather than the top-level `value` field used by standard metrics — `parse_response()` handles both formats with a fallback. `sonarqube_measures_history` also includes `new_*` metrics (via `HISTORY_METRICS`); the search_history endpoint may return date entries without values for these metrics, which are stored as NULL.

**Measures history windowing.** `sonarqube_measures_history` uses `from`/`to` date parameters to bound API requests. The `history_days_back` attribute (default: 720, overridable via `SONARQUBE_HISTORY_DAYS_BACK` env var) controls the maximum lookback. In FORWARD mode: `from = max(watermark, today - 720)`. The table accumulates data past this window naturally as runs add newer data.

**Measures history flattening.** The API returns metrics grouped by type (`{metric: "coverage", history: [{date, value}, ...]}`). `parse_response()` flattens this into one row per `(project_key, branch, metric_key, analysis_date)`.

**`collected_at` timestamp.** Both measures assets include a `collected_at` column set to the UTC timestamp when the data was fetched. For `sonarqube_measures` (live snapshot), this records when the snapshot was taken. For `sonarqube_measures_history`, it records when the historical data was pulled.

---

## ServiceNow

**Client:** [pysnc](https://github.com/ServiceNow/PySNC) (GlideRecord interface) — ServiceNow's official Python client
**Authentication:** OAuth2 password grant (username + password + client_id + client_secret) or Basic Auth (username + password)
**API docs:** https://docs.servicenow.com/bundle/latest/page/integrate/inbound-rest/concept/c_TableAPI.html

All ServiceNow assets share `ServiceNowTableAsset` base class. Extraction uses pysnc's `GlideRecord` with automatic pagination — bypassing the httpx API pipeline entirely. Authentication is handled by `ServiceNowTokenManager` (set as `token_manager_class` on the base), which provides credentials to pysnc via its `get_pysnc_auth()` method. This keeps auth in the standard `TokenManager` pattern used by all other sources. Subclasses only set `table_name` and `columns`.

**Extraction flow:** The `extract()` method on `ServiceNowTableAsset` creates a pysnc client via `_create_pysnc_client()` (which delegates to `ServiceNowTokenManager` for credentials), creates a `GlideRecord`, sets fields from `self.columns`, applies date filters, and iterates results in batches of 1000.

### ITSM tables

| Asset | Table | Load | SN Table | API Endpoint |
|-------|-------|------|----------|--------------|
| `servicenow_incidents` | `raw.servicenow_incidents` | UPSERT | `incident` | `/api/now/table/incident` |
| `servicenow_changes` | `raw.servicenow_changes` | UPSERT | `change_request` | `/api/now/table/change_request` |
| `servicenow_problems` | `raw.servicenow_problems` | UPSERT | `problem` | `/api/now/table/problem` |
| `servicenow_change_tasks` | `raw.servicenow_change_tasks` | UPSERT | `change_task` | `/api/now/table/change_task` |

### Service catalog

| Asset | Table | Load | SN Table | API Endpoint |
|-------|-------|------|----------|--------------|
| `servicenow_catalog_requests` | `raw.servicenow_catalog_requests` | UPSERT | `sc_request` | `/api/now/table/sc_request` |
| `servicenow_catalog_items` | `raw.servicenow_catalog_items` | UPSERT | `sc_req_item` | `/api/now/table/sc_req_item` |

### User directory

| Asset | Table | Load | SN Table | API Endpoint |
|-------|-------|------|----------|--------------|
| `servicenow_users` | `raw.servicenow_users` | UPSERT | `sys_user` | `/api/now/table/sys_user` |
| `servicenow_user_groups` | `raw.servicenow_user_groups` | UPSERT | `sys_user_group` | `/api/now/table/sys_user_group` |
| `servicenow_locations` | `raw.servicenow_locations` | UPSERT | `cmn_location` | `/api/now/table/cmn_location` |
| `servicenow_departments` | `raw.servicenow_departments` | UPSERT | `cmn_department` | `/api/now/table/cmn_department` |

### CMDB & hardware

| Asset | Table | Load | SN Table | API Endpoint |
|-------|-------|------|----------|--------------|
| `servicenow_cmdb_cis` | `raw.servicenow_cmdb_cis` | UPSERT | `cmdb_ci` | `/api/now/table/cmdb_ci` |
| `servicenow_hardware_assets` | `raw.servicenow_hardware_assets` | UPSERT | `alm_hardware` | `/api/now/table/alm_hardware` |

### Reference/decode tables

| Asset | Table | Load | SN Table | API Endpoint |
|-------|-------|------|----------|--------------|
| `servicenow_choices` | `raw.servicenow_choices` | FULL_REPLACE | `sys_choice` | `/api/now/table/sys_choice` |

**`sys_choice`** is the dropdown decode table for all ServiceNow fields. It maps raw coded values (e.g., `incident.state="1"`) to human-readable labels (e.g., `"New"`). Unlike other ServiceNow assets, it uses FULL_REPLACE with RunMode.FULL because it is a reference table with no reliable incremental sync.

### Key design choices

**Why keyset pagination?** Offset pagination on large ServiceNow tables is unreliable — records inserted/updated during extraction cause rows to be skipped or duplicated. Keyset pagination sorts by `sys_updated_on,sys_id` and filters from the last-seen record, eliminating drift.

**Why Sequential?** Keyset pagination is inherently sequential (each page's filter depends on the previous page's last record).

**Why UPSERT?** In FORWARD mode we filter by `sys_updated_on >= last_watermark`. Records may reappear if updated between runs, so we upsert by `sys_id`.

**Reference fields:** With `sysparm_exclude_reference_link=true`, reference fields like `assigned_to` return the raw `sys_id` string instead of a verbose JSON object. Join to dimension tables (`servicenow_users`, `servicenow_user_groups`, `servicenow_locations`) by `sys_id` for human-readable values. The `servicenow_users` asset has a unique index on `email` (the primary join key for downstream datasets) and a composite index on `(last_name, first_name)` for name lookups.

**Missing column detection:** If the ServiceNow instance doesn't return a declared column (e.g., field deprecated or ACL changed), `_batch_to_df()` raises a `ValueError` listing the missing columns. This surfaces schema mismatches early so the asset definition can be corrected. Extra columns returned by the API that aren't in the asset's `columns` list are silently dropped.

**Type coercion:** pysnc returns all field values as strings. `_batch_to_df()` automatically coerces them based on the declared column types:

- **Boolean** — `active` (users, user_groups), `inactive` (choices): string `"true"`/`"false"` → Python `True`/`False` via `pandas.Series.map()`.
- **Float** — `latitude`, `longitude` (locations): string → float via `pd.to_numeric(errors="coerce")`.
- **DateTime** — `opened_at`, `closed_at`, `sys_updated_on`, `last_login_time`, etc.: empty strings `""` (ServiceNow's representation of null datetimes) are replaced with `None`, then parsed via `pd.to_datetime(utc=True, errors="coerce")`. This prevents PostgreSQL `InvalidDatetimeFormat` errors.

The loader also applies a universal safety net (`_coerce_datetime_strings()` in `load/loader.py`) that detects datetime-like string columns by sampling and converts them. This catches datetime formats from all sources — both ISO 8601 (`2025-12-01T09:00:00Z`) and ServiceNow's space-separated format (`2025-12-01 09:00:00`).

**Unique index safety:** During promotion, the loader automatically converts empty strings to NULL in Text columns covered by unique indexes (`_nullify_empty_strings_for_unique_indexes()` in `load/loader.py`). This prevents `UniqueViolation` errors when the source system sends empty strings for missing values (e.g., service accounts with no email). Before creating indexes, the loader also checks for duplicate non-NULL values and logs a WARNING with sample duplicates. If a unique index still fails, it falls back to a non-unique index so the pipeline completes.

---

## GitHub

**Authentication:** GitHub App installation token (JWT → exchange) — `GitHubAppTokenManager`
**API docs:** https://docs.github.com/en/rest (most assets) · https://docs.github.com/en/graphql (`github_deployments`)

### Org-level assets (sequential)

| Asset | Table | Load | API Endpoint |
|-------|-------|------|--------------|
| `github_repos` | `raw.github_repos` | UPSERT | `/orgs/{org}/repos` |
| `github_members` | `raw.github_members` | UPSERT | `/orgs/{org}/members` |
| `github_runner_groups` | `raw.github_runner_groups` | UPSERT | `/orgs/{org}/actions/runner-groups` |

### Repo-scoped assets (entity-parallel off github_repos)

| Asset | Table | Load | API Endpoint | Entity Key Injection |
|-------|-------|------|--------------|---------------------|
| `github_pull_requests` | `raw.github_pull_requests` | UPSERT | `/repos/{repo}/pulls` | No (in response) |
| `github_branches` | `raw.github_branches` | FULL_REPLACE | `/repos/{repo}/branches` | Yes (`repo_full_name`) |
| `github_commits` | `raw.github_commits` | UPSERT | `/repos/{repo}/commits` | Yes (`repo_full_name`) |
| `github_workflows` | `raw.github_workflows` | FULL_REPLACE | `/repos/{repo}/actions/workflows` | Yes (`repo_full_name`) |
| `github_workflow_runs` | `raw.github_workflow_runs` | UPSERT | `/repos/{repo}/actions/runs` | Yes (`repo_full_name`) |
| `github_repo_properties` | `raw.github_repo_properties` | FULL_REPLACE | `/repos/{repo}/properties/values` | Yes (`repo_full_name`) |
| `github_deployments` | `raw.github_deployments` | UPSERT | `POST /graphql` (deployments connection) | Yes (`organization`, `repo_name`, `org_repo_key` via `entity_key_map`) |

### Deeper nested assets

| Asset | Table | Load | Parent | API Endpoint |
|-------|-------|------|--------|--------------|
| `github_user_details` | `raw.github_user_details` | FULL_REPLACE | `github_members` | `/users/{login}` |
| `github_workflow_jobs` | `raw.github_workflow_jobs` | UPSERT | `github_workflow_runs` | `/repos/{repo}/actions/runs/{run_id}/jobs` |
| `github_runner_group_repos` | `raw.github_runner_group_repos` | FULL_REPLACE | `github_runner_groups` | `/orgs/{org}/actions/runner-groups/{id}/repositories` |

### Architecture: Entity Key Injection

When an API response doesn't include the parent identifier (e.g., branches endpoint doesn't return `repo_full_name`), the `entity_key_column` attribute on the asset tells the extraction framework to inject the entity key as a column after parsing. For composite parent keys (e.g., SonarQube measures fanning out by project+branch), use `entity_key_map` instead — it maps dict fields from the parent's composite PK to DataFrame column names. Both are handled automatically by `_fetch_pages()` — no custom code needed in the asset.

### Key design choices

- **Multi-org:** One Airflow DAG per org. Each org can have its own GitHub App installation ID.
- **Case-insensitive org filtering:** `filter_to_current_org()` matches `GITHUB_ORGS` against repo names case-insensitively. Setting `GITHUB_ORGS=td-universe` correctly matches repos stored as `TD-Universe/repo-name`.
- **Empty repo handling:** `GitHubRepoAsset.classify_error()` returns `"skip"` for HTTP 409 (GitHub's response for empty/initializing repos with no commits or branches). The entity is skipped and extraction continues with the next repo.
- **Incremental commits:** Uses GitHub's `since` query parameter for efficient forward sync.
- **Incremental PRs/runs:** Sort by `updated desc` with `should_stop()` watermark detection (no `since` param available).
- **Workflow jobs:** Composite entity key `(repo_full_name, id)` loaded from `github_workflow_runs` table. The repo and run_id are used to build the URL; join to `github_workflow_runs` via `run_id` for repo context.
- **Deployments (GraphQL):** `github_deployments` is the first GraphQL-backed asset. It POSTs to `/graphql` and inherits `GitHubRepoAsset` (the base class is transport-agnostic). The full pattern — cursor plumbing, POST body shape, error-envelope guard — is the [GraphQL transport note](extending-reference.md#graphql-transport-note). Asset-specific details:
  - **Pagination:** cursor (`pageInfo.endCursor`), ordered `CREATED_AT DESC`.
  - **Stop rule:** `should_stop()` halts once the page's oldest deployment falls below `max(FORWARD watermark, today − pull_upto_days)`. Default `pull_upto_days = 720` (≈ 2 years) also caps FULL backfills.
  - **Description truncation:** descriptions over 4000 chars become `head(2000) + "[truncated]" + tail(2000)` in `transform()`; a per-run INFO log reports the count when truncation fires.
  - **Error guards:** a GraphQL `errors` envelope or a non-dict top-level body raises a typed `ValueError` naming the asset.
- **Shared helpers:** `get_github_org()`, `get_github_base_url()`, `filter_to_current_org()` in `assets/github/helpers.py`.
- **BigInteger IDs:** All GitHub `id` columns use `BigInteger()` (64-bit). GitHub's global ID counter has exceeded the 32-bit signed integer limit (2,147,483,647). Use `BigInteger()` for any new ID column from GitHub's API.
- **Notable column types:** `private` and `archived` (repos), `protected` (branches), `draft` (PRs), `default` and `allows_public_repositories` (runner_groups) are stored as native `Boolean` columns (not Text strings).

---

## Jira

**Authentication:** Cloud: email + API token (Basic Auth). Data Center: PAT (Bearer) — `JiraTokenManager`
**API docs:** https://developer.atlassian.com/cloud/jira/platform/rest/v3/

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `jira_projects` | `raw.jira_projects` | FULL_REPLACE | Sequential | offset (`startAt`, `maxResults`) | `/rest/api/3/project/search` |
| `jira_issues` | `raw.jira_issues` | UPSERT | ENTITY_PARALLEL (3 workers) | offset (`startAt`, `maxResults`) | `/rest/api/3/search` |

**Why Sequential for projects?** Project list is typically small (tens, not thousands). No need for parallelism.

**Why ENTITY_PARALLEL for issues?** Issues are scoped per project via JQL (`project = "KEY"`). We load project keys from `jira_projects` and fetch issues per project in parallel.

**Date filtering:** Jira issues use JQL: `updated >= "2025-01-01"`. The `_build_jql()` helper constructs the query dynamically.

**Notable column types:** `is_private` (projects) is stored as a native `Boolean` column.

---

## Transforms

| Asset | Table | Load | Source Tables | Description |
|-------|-------|------|---------------|-------------|
| `incident_summary` | `mart.incident_summary` | FULL_REPLACE | `servicenow_incidents` | Daily incident count grouped by priority and state |
| `sonarqube_adoption_trend` | `mart.sonarqube_adoption_trend` | FULL_REPLACE | `sonarqube_measures_history` | Weekly new-project onboardings with running cumulative, gap-free through the last completed ISO week (PowerBI-ready) |

**Why FULL_REPLACE?** The summary is always recomputed from the full incidents table. There's no incremental benefit since the aggregation covers all dates.

### Dialect-agnostic SQL

Transforms target both PostgreSQL 16+ and MariaDB 10.11+. The base `TransformAsset.query(context, dialect)` signature passes a `Dialect` instance so an asset can emit dialect-correct fragments via helpers instead of baking in Postgres-only syntax:

- `dialect.week_start_from_ts(expr)` — Monday-of-week DATE from a timestamp.
- `dialect.date_add_days(expr, n)` — date arithmetic.
- `dialect.cast_bigint(expr)` — 64-bit signed integer cast (`BIGINT` on Postgres, `SIGNED` on MariaDB).

`sonarqube_adoption_trend` uses these helpers plus a `WITH RECURSIVE` CTE (supported on both backends since Postgres 8.4 and MariaDB 10.2) to generate a gap-free week spine without Postgres-only `generate_series`.
