# Assets Catalog

This catalog documents every built-in asset. Use it as a reference when building new assets — find one with a similar pattern and use it as a starting point.

## SonarQube

**Authentication:** Static API token (Bearer) — `SonarQubeTokenManager`
**API docs:** https://next.sonarqube.com/sonarqube/web_api

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `sonarqube_projects` | `raw.sonarqube_projects` | FULL_REPLACE | Custom `extract()` | page_number (`p`, `ps`) + sharded `q` param | `/api/components/search?qualifiers=TRK` |
| `sonarqube_issues` | `raw.sonarqube_issues` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/issues/search` |
| `sonarqube_measures` | `raw.sonarqube_measures` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | none (one call per project) | `/api/measures/component` |

**SonarQube Issues and Measures** extend `SonarQubeAsset` (in `assets/sonarqube/helpers.py`), a shared base class that sets `token_manager_class`, `source_name`, `target_schema`, and `rate_limit_per_second`. **SonarQube Projects** extends `RestAsset` but overrides `extract()` to handle SonarQube's 10,000-result Elasticsearch limit via query sharding.

**Why custom `extract()` for projects?** SonarQube's `/api/components/search` endpoint is backed by Elasticsearch, which caps results at 10,000. For instances with >9,900 projects, standard pagination fails at page 101. The custom extraction shards queries using the `q` (name-substring) parameter with 2-character alphanumeric prefixes (1,296 combinations), recursively extending to 3+ characters for hot prefixes, and deduplicates by project key. Safety guards include: max pages per shard (100), max recursion depth (4), and a >5% shortfall abort to prevent FULL_REPLACE from overwriting complete data with partial results.

**Why ENTITY_PARALLEL for issues?** Issues are scoped per project (`componentKeys` param). We load the list of project keys from `sonarqube_projects` and fetch issues for each project in parallel.

**Why ENTITY_PARALLEL for measures?** Each project has its own set of measures. One API call per project returns all requested metrics (`ncloc`, `bugs`, `vulnerabilities`, `code_smells`, `coverage`, `duplicated_lines_density`, `sqale_index`).

**Why UPSERT for issues?** In FORWARD mode we fetch issues created since the last run. The same issue key might appear in multiple runs if it was updated, so we upsert by PK to avoid duplicates.

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
