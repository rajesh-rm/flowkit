# Configuration

## Database Connection

data-assets supports **PostgreSQL 16+** and **MariaDB 10.11+**. Set `DATABASE_URL` pointing to your database:

```bash
# PostgreSQL
export DATABASE_URL="postgresql://user:pass@host:5432/data_assets"

# MariaDB
export DATABASE_URL="mysql+pymysql://user:pass@host:3306/data_assets"
```

Install the appropriate driver:

```bash
pip install data-assets[postgres]   # PostgreSQL (psycopg2)
pip install data-assets[mariadb]    # MariaDB (PyMySQL)
```

**Backend detection**: The database type is auto-detected from the `DATABASE_URL` prefix. You can also set it explicitly:

```bash
export DATABASE_BACKEND=postgres   # or mariadb
```

If both `DATABASE_BACKEND` and `DATABASE_URL` are set and conflict (e.g., `DATABASE_BACKEND=mariadb` with a `postgresql://` URL), the package raises a `RuntimeError` at startup.

**Resolution order** for the connection string:
1. Airflow Connection `data_assets_db` (if Airflow is installed)
2. `DATABASE_URL` environment variable
3. `DATABASE_URL` in `.env` file

**MariaDB compatibility** — the following differences are handled automatically:

| Behavior | PostgreSQL | MariaDB | Handling |
|---|---|---|---|
| Text PKs | TEXT indexable | TEXT cannot be PK | Auto-converted to VARCHAR(255) |
| Timestamps | TIMESTAMPTZ (tz-aware) | DATETIME (tz-naive) | Stored as UTC on both |
| Datetime strings | Accepts ISO 8601 with 'Z' | Rejects 'Z' suffix | Auto-converted before write |
| Identifier quoting | `"double quotes"` | `` `backticks` `` | Dialect layer handles it |
| Index methods | GIN, GIST, BRIN, SPGIST | BTREE, HASH only | Falls back to BTREE |

No application code changes are needed — the dialect abstraction layer handles all differences.

## Source Credentials

### SonarQube

| Variable | Description |
|----------|-------------|
| `SONARQUBE_URL` | SonarQube server URL (e.g., `https://sonar.example.com`) |
| `SONARQUBE_TOKEN` | API token |

**How to get the token:** Log in to SonarQube → click your avatar (top-right) → **My Account** → **Security** tab → **Generate Tokens**. Choose type "User Token", give it a name, and copy the value.

### ServiceNow

ServiceNow assets use [pysnc](https://github.com/ServiceNow/PySNC) (GlideRecord client). Username and password are always required.

**Required:**

| Variable | Description |
|----------|-------------|
| `SERVICENOW_INSTANCE` | Instance URL (e.g., `https://dev12345.service-now.com`) |
| `SERVICENOW_USERNAME` | Username |
| `SERVICENOW_PASSWORD` | Password |

**Optional — OAuth2 (recommended for production):**

| Variable | Description |
|----------|-------------|
| `SERVICENOW_CLIENT_ID` | OAuth2 client ID |
| `SERVICENOW_CLIENT_SECRET` | OAuth2 client secret |

When all four credentials are set, `ServiceNowTokenManager.get_pysnc_auth()` returns a `ServiceNowPasswordGrantFlow` for OAuth2. Otherwise it falls back to basic auth with `(username, password)`. Credentials are resolved via `CredentialResolver` (Airflow Connections → env vars → `.env` file) — the same mechanism used by all other source token managers.

**How to set up OAuth2:** In ServiceNow, navigate to **System OAuth > Application Registry** → **Create an OAuth API endpoint for external clients**. Note the Client ID and Client Secret. pysnc uses the `password` grant type, which requires all five variables (instance, username, password, client_id, client_secret).

### GitHub

| Variable | Description |
|----------|-------------|
| `GITHUB_APP_ID` | GitHub App ID |
| `GITHUB_PRIVATE_KEY` | PEM-encoded private key |
| `GITHUB_INSTALLATION_ID` | Installation ID for the target org(s) |
| `GITHUB_ORGS` | Comma-separated org names (e.g., `"org1,org2"`). Case-insensitive for entity filtering — `TD-Universe` and `td-universe` both match |
| `GITHUB_API_URL` | Optional API URL override (default: `https://api.github.com`) |

**How to set up a GitHub App:**
1. Go to **Settings > Developer settings > GitHub Apps > New GitHub App**
2. Set permissions: Repository (read), Pull requests (read), Actions (read)
3. After creating, note the **App ID** from the app's settings page
4. Generate a **private key** (PEM file) — download and store securely
5. Install the app on your org(s) — note the **Installation ID** from the URL (`/installations/{id}`)
6. Set `GITHUB_PRIVATE_KEY` to the full PEM content (including `-----BEGIN RSA PRIVATE KEY-----`)

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

**How to get Jira Cloud API token:** Log in to https://id.atlassian.com/manage-profile/security/api-tokens → **Create API token**. Use your email as `JIRA_EMAIL` and the token as `JIRA_API_TOKEN`.

**How to get Jira Data Center PAT:** Log in → **Profile** → **Personal Access Tokens** → **Create token**.

## Passing Secrets from Airflow

Instead of pre-setting env vars on workers, pass secrets explicitly from Airflow
Connections via the `secrets` parameter:

```python
from airflow.sdk import BaseHook
from data_assets import run_asset

conn = BaseHook.get_connection("sonarqube")
run_asset("sonarqube_projects", secrets={
    "SONARQUBE_URL": f"https://{conn.host}",
    "SONARQUBE_TOKEN": conn.password,
})
```

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

With a secret backend (Vault, AWS SSM), Airflow resolves these at runtime — values
never touch the metadata DB.

## Network and Proxy

If your environment routes traffic through a corporate proxy, set these variables
so that `httpx` (the HTTP client used for API extraction) and `pysnc` (ServiceNow
client) can reach external APIs.

| Variable | Description |
|----------|-------------|
| `HTTPS_PROXY` | Proxy URL for HTTPS traffic (e.g., `http://proxy.corp.example.com:8080`) |
| `HTTP_PROXY` | Proxy URL for HTTP traffic (same as above in most setups) |
| `NO_PROXY` | Comma-separated hostnames/domains to bypass the proxy (e.g., `localhost,127.0.0.1,.corp.example.com`) |
| `SSL_CERT_FILE` | Path to custom CA bundle for TLS inspection (e.g., `/etc/pki/tls/certs/corporate-ca-bundle.pem`) |
| `REQUESTS_CA_BUNDLE` | Same as `SSL_CERT_FILE` — used by `requests` and `httpx` |

`httpx` respects `HTTPS_PROXY` and `SSL_CERT_FILE` natively. If your corporate
proxy's CA is already in the system trust store, no extra configuration is needed.

For **development tooling** proxy setup (uv, pip, git), see [quickstart-dev.md](quickstart-dev.md#2-enterprise-proxy-setup-corporate-networks-only).

## Database Retry Configuration

The framework automatically retries transient database errors during critical write operations (`write_to_temp`, `promote`, `save_checkpoint`). Configure via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_ASSETS_DB_RETRY_ATTEMPTS` | `3` | Maximum retry attempts before failing the run |
| `DATA_ASSETS_DB_RETRY_BASE_DELAY` | `2.0` | Initial delay in seconds; doubles each attempt (exponential backoff, capped at 30s) |

Retryable errors: `OperationalError`, `DisconnectionError`, `ConnectionError`, `TimeoutError`. Non-retryable errors (`IntegrityError`, `ProgrammingError`) fail immediately without retry.

On exhaustion, the run fails with `DatabaseRetryExhausted`. Logs include the number of attempts, total wait time, and the last underlying error — useful for Airflow admin triage.

## Runtime Overrides

Pass overrides as keyword arguments to `run_asset()`. All overrides are optional — omitting them uses the asset's class-level defaults.

```python
run_asset(
    "sonarqube_issues",
    run_mode="forward",
    rate_limit_per_second=2.0,   # Slower during business hours
    max_workers=2,                # Reduce parallelism
    max_pages=5,                  # Developer testing: fetch only 5 pages
    dry_run=True,                 # Skip DB write (extract + validate only)
    start_date=some_datetime,     # Override date window
    end_date=some_datetime,
    partition_key="org-one",      # Multi-org: scope locks + watermarks
)
```

### All supported overrides

| Override | Type | Description |
|----------|------|-------------|
| `run_mode` | `str` | `"full"`, `"forward"`, `"backfill"`, or `"transform"` |
| `partition_key` | `str` | Scope locks/watermarks/checkpoints to a partition (multi-org) |
| `secrets` | `dict` | Env vars injected for this run (from Airflow Connections, etc.) |
| `dry_run` | `bool` | Extract and validate but skip promotion to target table |
| `max_pages` | `int` | **Developer testing**: stop after N pages (see below) |
| `max_entities` | `int` | **Developer testing**: limit entity count for entity-parallel (see below) |
| `rate_limit_per_second` | `float` | Override the asset's API rate limit |
| `max_workers` | `int` | Override thread count for parallel extraction modes |
| `request_timeout` | `float` | HTTP request timeout in seconds |
| `max_retries` | `int` | Max retry attempts on transient errors |
| `start_date` | `datetime` | Override the computed start of the extraction window |
| `end_date` | `datetime` | Override the computed end of the extraction window |
| `airflow_run_id` | `str` | Links this run to an Airflow DAG run in `run_history` |

### max_pages — developer testing

`max_pages` limits how many pages the extractor fetches before stopping. This is useful when validating a new asset or debugging against a real API without waiting for a full multi-hour run.

```python
# Fetch at most 3 pages, skip the DB write
run_asset("github_prs", max_pages=3, dry_run=True)
```

Behavior per extraction mode:

| Mode | What `max_pages=N` means |
|------|--------------------------|
| Sequential | Stop after N API calls |
| Page-parallel (e.g., Jira issues) | Fetch N pages total across all workers |
| Entity-parallel (e.g., GitHub PRs) | Each entity (repo) gets at most N pages |
| ServiceNow | Stop after N batches of 1,000 records |
| SonarQube Projects | Cap each pagination shard at N pages |

> **Do not use `max_pages` in production.** Partial data will overwrite the full dataset with `FULL_REPLACE`, and can leave `UPSERT` tables incomplete. Use `dry_run=True` alongside `max_pages` to prevent any writes to the target table.

### max_entities — developer testing

`max_entities` limits how many parent entities the extractor processes in entity-parallel mode. This is useful when the parent table has thousands of entities (e.g., 52K repos) and you want a quick smoke test without waiting for all of them.

```python
# Process only the first 10 repos, 1 page each, skip the DB write
run_asset("github_commits", max_entities=10, max_pages=1, dry_run=True)
```

The slice is applied **after** `filter_entity_keys()`, so the limited set only includes entities from the correct org/partition.

> **Do not use `max_entities` in production.** It produces incomplete data. Always pair with `dry_run=True`.
