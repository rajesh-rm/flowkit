# Configuration

## Database Connection

Set `DATABASE_URL` environment variable pointing to your Postgres instance:

```bash
export DATABASE_URL="postgresql://user:pass@host:5432/data_assets"
```

Resolution order:
1. Airflow Connection `data_assets_db` (if Airflow is installed)
2. `DATABASE_URL` environment variable
3. `DATABASE_URL` in `.env` file

## Source Credentials

### SonarQube

| Variable | Description |
|----------|-------------|
| `SONARQUBE_URL` | SonarQube server URL (e.g., `https://sonar.example.com`) |
| `SONARQUBE_TOKEN` | API token (User > My Account > Security) |

### ServiceNow

**OAuth2 (preferred):**

| Variable | Description |
|----------|-------------|
| `SERVICENOW_INSTANCE` | Instance URL (e.g., `https://dev12345.service-now.com`) |
| `SERVICENOW_CLIENT_ID` | OAuth2 client ID |
| `SERVICENOW_CLIENT_SECRET` | OAuth2 client secret |

**Basic Auth (fallback):**

| Variable | Description |
|----------|-------------|
| `SERVICENOW_INSTANCE` | Instance URL |
| `SERVICENOW_USERNAME` | Username |
| `SERVICENOW_PASSWORD` | Password |

### GitHub

| Variable | Description |
|----------|-------------|
| `GITHUB_APP_ID` | GitHub App ID |
| `GITHUB_PRIVATE_KEY` | PEM-encoded private key |
| `GITHUB_INSTALLATION_ID` | Installation ID for the target org(s) |
| `GITHUB_ORGS` | Comma-separated org names (e.g., `"org1,org2"`) |
| `GITHUB_API_URL` | Optional API URL override (default: `https://api.github.com`) |

### Jira

**Cloud (email + API token):**

| Variable | Description |
|----------|-------------|
| `JIRA_URL` | Jira instance URL (e.g., `https://mysite.atlassian.net`) |
| `JIRA_EMAIL` | User email |
| `JIRA_API_TOKEN` | API token |

**Data Center (PAT):**

| Variable | Description |
|----------|-------------|
| `JIRA_URL` | Jira Data Center URL |
| `JIRA_PAT` | Personal access token |

## Runtime Overrides

Pass overrides as keyword arguments to `run_asset()`:

```python
run_asset(
    "sonarqube_issues",
    run_mode="forward",
    rate_limit_per_second=2.0,   # Slower during business hours
    max_workers=2,                # Reduce parallelism
    start_date=some_datetime,     # Override date window
    end_date=some_datetime,
)
```
