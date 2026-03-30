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
| SonarQube | projects, issues | Code quality data |
| ServiceNow | incidents, changes | ITSM data |
| GitHub | repos, pull_requests | Source control data (multi-org) |
| Jira | projects, issues | Project tracking data |
| Transforms | incident_summary | Postgres-to-Postgres derived tables |

## Key Features

- **Atomic runs**: temp table per run, promote to main only on success
- **Resumable extraction**: checkpoint-based retry without re-fetching
- **Parallel extraction**: page-parallel and entity-parallel thread pool modes
- **Self-managing schemas**: auto-create tables, additive column migration
- **In-process rate limiting**: thread-safe token bucket per DAG
- **Token management**: pluggable per-source (GitHub App, ServiceNow OAuth, SonarQube static, Jira Cloud/DC)

## Documentation

- [**Local Dev Quickstart**](docs/quickstart-dev.md) — get running in 5 minutes with uv
- [Architecture](docs/architecture.md) — design and lifecycle
- [User Guide](docs/user-guide.md) — installation and setup
- [Configuration](docs/configuration.md) — credentials and runtime overrides
- [Assets Catalog](docs/assets-catalog.md) — all built-in assets
- [Extending](docs/extending.md) — adding new sources and transforms

## Requirements

- Python 3.11+
- PostgreSQL 14+
- Apache Airflow 2.x+ (for scheduling)

## License

Apache License 2.0
