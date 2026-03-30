# Assets Catalog

## SonarQube

| Asset | URI | Default Schedule | Description |
|-------|-----|-----------------|-------------|
| `sonarqube_issues` | `engmx://sonarqube/issues` | `@daily` | Code quality issues — bugs, vulnerabilities, code smells |
| `sonarqube_metrics` | `engmx://sonarqube/metrics` | `@daily` | Project-level quality metrics (coverage, duplications, complexity) |
| `sonarqube_quality_gates` | `engmx://sonarqube/quality_gates` | `@daily` | Quality gate status per project |

### Connection: `sonarqube_default`

- `host` — SonarQube server URL (e.g., `https://sonar.example.com`)
- `password` — API token (generated under User > My Account > Security)

### Extra Params

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `project_keys` | list[str] | all projects | Filter to specific project keys |
| `severities` | list[str] | all | Filter issues by severity (BLOCKER, CRITICAL, MAJOR, MINOR, INFO) |

---

## ServiceNow

| Asset | URI | Default Schedule | Description |
|-------|-----|-----------------|-------------|
| `servicenow_incidents` | `engmx://servicenow/incidents` | `@hourly` | Incident records |
| `servicenow_changes` | `engmx://servicenow/changes` | `@daily` | Change request records |
| `servicenow_cmdb_items` | `engmx://servicenow/cmdb_items` | `@weekly` | Configuration items from CMDB |

### Connection: `servicenow_default`

- `host` — Instance URL (e.g., `https://instance.service-now.com`)
- `login` — Username
- `password` — Password

### Extra Params

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `table` | str | varies per asset | ServiceNow table name |
| `query` | str | `""` | Encoded query string (e.g., `active=true`) |
| `fields` | list[str] | all | Limit returned fields |

---

## GitHub

| Asset | URI | Default Schedule | Description |
|-------|-----|-----------------|-------------|
| `github_repositories` | `engmx://github/repositories` | `@daily` | Repository metadata |
| `github_pull_requests` | `engmx://github/pull_requests` | `@daily` | Pull requests with review info |
| `github_commits` | `engmx://github/commits` | `@daily` | Commit history |
| `github_actions_runs` | `engmx://github/actions_runs` | `@daily` | GitHub Actions workflow run data |

### Connection: `github_default`

- `host` — API URL (`https://api.github.com` or GitHub Enterprise URL)
- `password` — Personal access token (PAT)

### Extra Params

| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `orgs` | list[str] | required | GitHub organizations to query |
| `include_archived` | bool | `false` | Include archived repositories |
| `since_days` | int | `7` | For commits/PRs, look back this many days |
