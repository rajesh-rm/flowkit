# Assets Catalog

This catalog documents every built-in asset. Use it as a reference when building new assets — find one with a similar pattern and use it as a starting point.

## SonarQube

**Authentication:** Static API token (Bearer) — `SonarQubeTokenManager`
**API docs:** https://next.sonarqube.com/sonarqube/web_api

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `sonarqube_projects` | `raw.sonarqube_projects` | FULL_REPLACE | PAGE_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/components/search?qualifiers=TRK` | **RestAsset** |
| `sonarqube_issues` | `raw.sonarqube_issues` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/issues/search` |
| `sonarqube_measures` | `raw.sonarqube_measures` | FULL_REPLACE | ENTITY_PARALLEL (3 workers) | none (one call per project) | `/api/measures/component` |

**SonarQube projects uses RestAsset** (declarative) with a `build_request` override to add `qualifiers=TRK`. The endpoint `/api/components/search` returns all component types; the qualifier filter scopes to projects only.

**Why PAGE_PARALLEL for projects?** The first response includes `paging.total` so we know how many pages exist upfront and can fan out.

**Why ENTITY_PARALLEL for issues?** Issues are scoped per project (`componentKeys` param). We load the list of project keys from `sonarqube_projects` and fetch issues for each project in parallel.

**Why ENTITY_PARALLEL for measures?** Each project has its own set of measures. One API call per project returns all requested metrics (`ncloc`, `bugs`, `vulnerabilities`, `code_smells`, `coverage`, `duplicated_lines_density`, `sqale_index`).

**Why UPSERT for issues?** In FORWARD mode we fetch issues created since the last run. The same issue key might appear in multiple runs if it was updated, so we upsert by PK to avoid duplicates.

**Not yet implemented** (from the reference API): `/api/project_branches/list` and `/api/project_analyses/search`. These can be built as entity-parallel assets using `entity_key_column = "project_key"` — the same pattern used by GitHub branches, commits, and workflows.

---

## ServiceNow

**Authentication:** OAuth2 client_credentials or Basic Auth — `ServiceNowTokenManager`
**API docs:** https://docs.servicenow.com/bundle/latest/page/integrate/inbound-rest/concept/c_TableAPI.html

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `servicenow_incidents` | `raw.servicenow_incidents` | UPSERT | Sequential | keyset (`sys_updated_on`, `sys_id`) | `/api/now/table/incident` |
| `servicenow_changes` | `raw.servicenow_changes` | UPSERT | Sequential | keyset (`sys_updated_on`, `sys_id`) | `/api/now/table/change_request` |

**Both ServiceNow assets share `ServiceNowTableAsset`** base class — `build_request()` and `parse_response()` are defined once. Subclasses only set `table_name` and `columns`.

**Why keyset pagination?** Offset pagination on large ServiceNow tables is unreliable — records inserted/updated during extraction cause rows to be skipped or duplicated. Keyset pagination sorts by `sys_updated_on,sys_id` and filters from the last-seen record, eliminating drift.

**Why Sequential?** Keyset pagination is inherently sequential (each page's filter depends on the previous page's last record).

**Why UPSERT?** In FORWARD mode we filter by `sys_updated_on >= last_watermark`. Records may reappear if updated between runs, so we upsert by `sys_id`.

---

## GitHub

**Authentication:** GitHub App installation token (JWT → exchange) — `GitHubAppTokenManager`
**API docs:** https://docs.github.com/en/rest

### Org-level assets (sequential)

| Asset | Table | Load | API Endpoint |
|-------|-------|------|--------------|
| `github_repos` | `raw.github_repos` | UPSERT | `/orgs/{org}/repos` |
| `github_members` | `raw.github_members` | FULL_REPLACE | `/orgs/{org}/members` |
| `github_runner_groups` | `raw.github_runner_groups` | FULL_REPLACE | `/orgs/{org}/actions/runner-groups` |

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
- **Workflow jobs:** Composite entity key `(repo_full_name, run_id)` loaded from `github_workflow_runs` table via `_load_entity_keys()`.
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
