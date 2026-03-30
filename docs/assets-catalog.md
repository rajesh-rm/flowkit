# Assets Catalog

## SonarQube

| Asset | Table | Mode | Parallel | Description |
|-------|-------|------|----------|-------------|
| `sonarqube_projects` | `raw.sonarqube_projects` | FULL | PAGE_PARALLEL (3) | All SonarQube projects |
| `sonarqube_issues` | `raw.sonarqube_issues` | FORWARD | ENTITY_PARALLEL (3) | Code quality issues per project |

## ServiceNow

| Asset | Table | Mode | Parallel | Description |
|-------|-------|------|----------|-------------|
| `servicenow_incidents` | `raw.servicenow_incidents` | FORWARD | PAGE_PARALLEL (3) | Incident records |
| `servicenow_changes` | `raw.servicenow_changes` | FORWARD | PAGE_PARALLEL (3) | Change request records |

## GitHub

| Asset | Table | Mode | Parallel | Description |
|-------|-------|------|----------|-------------|
| `github_repos` | `raw.github_repos` | FULL | Sequential | Repos across configured orgs |
| `github_pull_requests` | `raw.github_pull_requests` | FORWARD | ENTITY_PARALLEL (4) | PRs per repository |

## Jira

| Asset | Table | Mode | Parallel | Description |
|-------|-------|------|----------|-------------|
| `jira_projects` | `raw.jira_projects` | FULL | Sequential | All Jira projects |
| `jira_issues` | `raw.jira_issues` | FORWARD | ENTITY_PARALLEL (3) | Issues per project |

## Transforms

| Asset | Table | Mode | Source Tables | Description |
|-------|-------|------|---------------|-------------|
| `incident_summary` | `mart.incident_summary` | TRANSFORM | `servicenow_incidents` | Daily incident count by priority/state |
