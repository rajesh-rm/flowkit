# Configuration

## Overview

Configuration has two layers:

1. **Airflow Connections** — credentials and host URLs for each source (managed by Airflow)
2. **FlowkitConfig** — package behavior: which sources are enabled, schedule overrides, DAG naming

## Airflow Connections

Each source maps to a default Airflow Connection ID. Users set these up via Airflow UI, CLI, or environment variables.

| Source | Default Connection ID | Type | Key Fields |
|--------|----------------------|------|------------|
| SonarQube | `sonarqube_default` | `http` | `host` = server URL, `password` = API token |
| ServiceNow | `servicenow_default` | `http` | `host` = instance URL, `login` = username, `password` = password |
| GitHub | `github_default` | `http` | `host` = `api.github.com` (or GHE URL), `password` = personal access token |

### Example: Setting up connections via CLI

```bash
airflow connections add sonarqube_default \
  --conn-type http \
  --conn-host https://sonar.example.com \
  --conn-password "sqa_xxxxx"

airflow connections add servicenow_default \
  --conn-type http \
  --conn-host https://instance.service-now.com \
  --conn-login admin \
  --conn-password "password"

airflow connections add github_default \
  --conn-type http \
  --conn-host https://api.github.com \
  --conn-password "ghp_xxxxx"
```

## Package Configuration

### FlowkitConfig Schema

```yaml
# flowkit_config.yaml
dag_prefix: "engmx"                    # Prefix for generated DAG IDs (default: "flowkit")
tags:                                    # Tags applied to all generated DAGs
  - "engmx"
  - "data-asset"

sources:
  sonarqube:
    enabled: true                        # Enable/disable this source (default: true)
    connection_id: "sonarqube_default"   # Override default connection ID
    schedule: "0 6 * * *"               # Override default schedule
    extra_params:                        # Source-specific parameters
      project_keys:
        - "my-project-1"
        - "my-project-2"

  servicenow:
    enabled: true
    connection_id: "snow_prod"
    schedule: "@hourly"
    extra_params:
      table: "incident"
      query: "active=true"

  github:
    enabled: true
    connection_id: "github_default"
    extra_params:
      orgs:
        - "my-org"
      include_archived: false
```

### Loading Order

Configuration is resolved in this priority (highest first):

1. **Programmatic** — `FlowkitConfig` passed directly to `generate_dags(config=...)`
2. **Config file** — path set via `FLOWKIT_CONFIG_PATH` environment variable
3. **Airflow Variable** — JSON stored in Airflow Variable named `flowkit_config`
4. **Defaults** — sensible defaults baked into each `AssetDefinition`

### Per-Source Configuration

Each source entry under `sources` accepts:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | `true` | Whether to generate DAGs for this source |
| `connection_id` | string | source-specific | Airflow Connection ID to use |
| `schedule` | string | source-specific | Cron expression or Airflow preset (`@daily`, `@hourly`) |
| `extra_params` | dict | `{}` | Source-specific parameters passed to the API client |
