# data_assets

Self-contained ETL engine for data assets, backed by PostgreSQL, orchestrated by Apache Airflow.

Airflow calls `run_asset(name, mode)` — this package handles everything else: extraction with rate limiting and parallelism, checkpointing for resumable retries, schema management, validation, and atomic promotion.

## Quick Start

```python
from data_assets import run_asset

result = run_asset("sonarqube_projects", run_mode="full")
```

## Built-in Sources

| Source | Assets | Description |
|--------|--------|-------------|
| GitHub | repos, pull_requests, branches, commits, workflows, workflow_runs, workflow_jobs, members, user_details, runner_groups, runner_group_repos, repo_properties | Source control, CI/CD, and org data (multi-org) |
| SonarQube | projects, issues, measures | Code quality data |
| ServiceNow | incidents, changes | ITSM data |
| Jira | projects, issues | Project tracking data |
| Transforms | incident_summary | Postgres-to-Postgres derived tables |

## Key Features

- **Atomic runs**: temp table per run, promote to main only on success
- **Resumable extraction**: checkpoint-based retry without re-fetching
- **Parallel extraction**: page-parallel and entity-parallel thread pool modes
- **Self-managing schemas**: auto-create tables, additive column migration
- **In-process rate limiting**: thread-safe sliding-window per DAG
- **Token management**: pluggable per-source (GitHub App, ServiceNow OAuth, SonarQube static, Jira Cloud/DC)
- **RestAsset pattern**: declarative asset definition for standard REST APIs (~25 lines, no custom code)
- **Dry run mode**: extract and validate without promoting to main table
- **Stale-run takeover**: automatic recovery from orphaned runs via heartbeat monitoring

## Documentation

Read in this order:

1. [**Local Dev Quickstart**](docs/quickstart-dev.md) — get running in 5 minutes
2. [**User Guide**](docs/user-guide.md) — run assets, understand run modes
3. [**Airflow Deployment**](docs/airflow-deployment.md) — deploy DAGs, zero-touch updates, admin overrides
4. [**Architecture**](docs/architecture.md) — how the ETL lifecycle works
5. [**Configuration**](docs/configuration.md) — credentials and runtime overrides
6. [**Assets Catalog**](docs/assets-catalog.md) — all built-in assets and their design choices
7. [**Extending**](docs/extending.md) — adding new sources, transforms, and token managers
8. [**Testing Guide**](docs/testing.md) — test structure, patterns, fixtures, and how to write tests

See [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow, code style, and PR guidelines.

## Requirements

- Python 3.11+
- PostgreSQL 14+
- Apache Airflow 2.x+ (for scheduling)

## License

Apache License 2.0
