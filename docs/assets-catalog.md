# Assets Catalog

This catalog documents every built-in asset. Use it as a reference when building new assets — find one with a similar pattern and use it as a starting point.

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
| `sonarqube_measures` | `raw.sonarqube_measures` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | none (one call per project) | `/api/measures/component` |
| `sonarqube_measures_history` | `raw.sonarqube_measures_history` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/measures/search_history` |

### Issues and analysis

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `sonarqube_issues` | `raw.sonarqube_issues` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/issues/search` |
| `sonarqube_analyses` | `raw.sonarqube_analyses` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/project_analyses/search` |
| `sonarqube_analysis_events` | `raw.sonarqube_analysis_events` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/project_analyses/search` |
| `sonarqube_branches` | `raw.sonarqube_branches` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | none (all branches in one call) | `/api/project_branches/list` |

### Data dependency graph

All entity-parallel assets depend on `sonarqube_projects` — the runner loads project keys from `raw.sonarqube_projects` and fans out one request per project.

```
sonarqube_projects
├── sonarqube_project_details
├── sonarqube_measures
├── sonarqube_measures_history
├── sonarqube_issues
├── sonarqube_analyses
├── sonarqube_analysis_events
└── sonarqube_branches
```

Run `sonarqube_projects` first (or let the DAG factory handle ordering).

### Key design choices

**Shared base class.** All entity-parallel assets extend `SonarQubeAsset` (in `assets/sonarqube/helpers.py`), which sets `token_manager_class`, `source_name`, `target_schema`, and `rate_limit_per_second` (5 req/s). It also provides an `api_url` property that resolves `SONARQUBE_URL` from env vars, and a shared `DEFAULT_METRICS` constant used by both measures assets. `SonarQubeProjects` uses `RestAsset` instead and overrides `extract()` for the sharding logic.

**Why custom `extract()` for projects?** SonarQube's `/api/components/search` is backed by Elasticsearch, which caps results at 10,000. For instances with >9,900 projects, standard pagination fails. The custom extraction shards queries using the `q` parameter with 2-character alphanumeric prefixes, recursively extending for hot prefixes, and deduplicates by project key. Safety guards: max pages per shard, max recursion depth (4), >5% shortfall abort.

**Why a separate project_details asset?** `/api/components/search` only returns `key`, `name`, `qualifier`. Richer metadata (description, visibility, version, tags, analysis dates) requires a per-project call to `/api/components/show`.

**Why UPSERT for issues, analyses, and measures_history?** These assets run in FORWARD mode — they fetch only data created/updated since the last watermark. The same key may appear in overlapping date windows, so UPSERT by PK prevents duplicates.

**Why FULL_REPLACE for branches and analysis_events?** Branches can be renamed or deleted between runs — FULL_REPLACE ensures stale branches are removed. Analysis events have no date column for incremental tracking, so a full refresh is the correct strategy.

**Why two assets for the same API (analyses + events)?** `/api/project_analyses/search` returns analyses with nested events. Rather than using JSONB or a custom multi-table `extract()` override, the events are extracted as a separate normalized asset. Both assets call the same endpoint independently — the API is hit twice per project, which is acceptable at 5 req/s with 3 workers. This keeps both assets in the standard entity-parallel pipeline with no custom extract logic.

**Measures history flattening.** The API returns metrics grouped by type (`{metric: "coverage", history: [{date, value}, ...]}`). `parse_response()` flattens this into one row per `(project_key, metric, date)`. In FORWARD mode, the `from` parameter filters to only new data points since the last watermark.

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

**Reference fields:** With `sysparm_exclude_reference_link=true`, reference fields like `assigned_to` return the raw `sys_id` string instead of a verbose JSON object. Join to dimension tables (`servicenow_users`, `servicenow_user_groups`, `servicenow_locations`) by `sys_id` for human-readable values.

**Missing column detection:** If the ServiceNow instance doesn't return a declared column (e.g., field deprecated or ACL changed), the framework logs a WARNING listing the missing columns. The extraction continues with those columns as NULL.

---

## GitHub

**Authentication:** GitHub App installation token (JWT → exchange) — `GitHubAppTokenManager`
**API docs:** https://docs.github.com/en/rest

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

### Deeper nested assets

| Asset | Table | Load | Parent | API Endpoint |
|-------|-------|------|--------|--------------|
| `github_user_details` | `raw.github_user_details` | FULL_REPLACE | `github_members` | `/users/{login}` |
| `github_workflow_jobs` | `raw.github_workflow_jobs` | UPSERT | `github_workflow_runs` | `/repos/{repo}/actions/runs/{run_id}/jobs` |
| `github_runner_group_repos` | `raw.github_runner_group_repos` | FULL_REPLACE | `github_runner_groups` | `/orgs/{org}/actions/runner-groups/{id}/repositories` |

### Architecture: Entity Key Injection

When an API response doesn't include the parent identifier (e.g., branches endpoint doesn't return `repo_full_name`), the `entity_key_column` attribute on the asset tells the extraction framework to inject the entity key as a column after parsing. This is handled automatically by `_fetch_pages()` — no custom code needed in the asset.

### Key design choices

- **Multi-org:** One Airflow DAG per org. Each org can have its own GitHub App installation ID.
- **Incremental commits:** Uses GitHub's `since` query parameter for efficient forward sync.
- **Incremental PRs/runs:** Sort by `updated desc` with `should_stop()` watermark detection (no `since` param available).
- **Workflow jobs:** Composite entity key `(repo_full_name, id)` loaded from `github_workflow_runs` table. The repo and run_id are used to build the URL; join to `github_workflow_runs` via `run_id` for repo context.
- **Shared helpers:** `get_github_org()`, `get_github_base_url()`, `filter_to_current_org()` in `assets/github/helpers.py`.

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

---

## Transforms

| Asset | Table | Load | Source Tables | Description |
|-------|-------|------|---------------|-------------|
| `incident_summary` | `mart.incident_summary` | FULL_REPLACE | `servicenow_incidents` | Daily incident count grouped by priority and state |

**Why FULL_REPLACE?** The summary is always recomputed from the full incidents table. There's no incremental benefit since the aggregation covers all dates.
