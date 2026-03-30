# Assets Catalog

This catalog documents every built-in asset. Use it as a reference when building new assets — find one with a similar pattern and use it as a starting point.

## SonarQube

**Authentication:** Static API token (Bearer) — `SonarQubeTokenManager`
**API docs:** https://next.sonarqube.com/sonarqube/web_api

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `sonarqube_projects` | `raw.sonarqube_projects` | FULL_REPLACE | PAGE_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/projects/search` | **RestAsset** |
| `sonarqube_issues` | `raw.sonarqube_issues` | UPSERT | ENTITY_PARALLEL (3 workers) | page_number (`p`, `ps`) | `/api/issues/search` |

**SonarQube projects uses RestAsset** (declarative) — no custom `build_request` or `parse_response`. The endpoint, pagination, and field mapping are all declared as class attributes. This is the simplest asset pattern in the codebase.

**Why PAGE_PARALLEL for projects?** The first response includes `paging.total` so we know how many pages exist upfront and can fan out.

**Why ENTITY_PARALLEL for issues?** Issues are scoped per project (`componentKeys` param). We load the list of project keys from `sonarqube_projects` and fetch issues for each project in parallel.

**Why UPSERT for issues?** In FORWARD mode we fetch issues created since the last run. The same issue key might appear in multiple runs if it was updated, so we upsert by PK to avoid duplicates.

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

| Asset | Table | Load | Parallel | Pagination | API Endpoint |
|-------|-------|------|----------|------------|--------------|
| `github_repos` | `raw.github_repos` | FULL_REPLACE | Sequential | page_number (`page`, `per_page`) | `/orgs/{org}/repos` |
| `github_pull_requests` | `raw.github_pull_requests` | UPSERT | ENTITY_PARALLEL (4 workers) | page_number (`page`, `per_page`) | `/repos/{owner}/{repo}/pulls` |

**Why Sequential for repos?** Repos are fetched per-org (from `GITHUB_ORGS` env var). The asset iterates through orgs sequentially, paginating within each. Multi-org state is tracked via checkpoint `{org_idx, page}`.

**Why ENTITY_PARALLEL for PRs?** PRs are per-repository. We load repo `full_name` values from `github_repos` and fetch PRs for each repo in parallel.

**Note:** GitHub PRs endpoint does NOT support a `since` query param. PRs are sorted by `updated desc` and `should_stop()` halts pagination when all PRs on a page are older than the watermark.

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
