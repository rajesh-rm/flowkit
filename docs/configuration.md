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

When all four credentials are set, pysnc uses the OAuth2 **password grant** flow. Otherwise it falls back to basic auth with username + password.

**How to set up OAuth2:** In ServiceNow, navigate to **System OAuth > Application Registry** → **Create an OAuth API endpoint for external clients**. Note the Client ID and Client Secret. pysnc uses the `password` grant type, which requires all four variables above.

### GitHub

| Variable | Description |
|----------|-------------|
| `GITHUB_APP_ID` | GitHub App ID |
| `GITHUB_PRIVATE_KEY` | PEM-encoded private key |
| `GITHUB_INSTALLATION_ID` | Installation ID for the target org(s) |
| `GITHUB_ORGS` | Comma-separated org names (e.g., `"org1,org2"`) |
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
from airflow.hooks.base import BaseHook
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
