# data_assets

Self-contained ETL engine for data assets, backed by PostgreSQL, orchestrated by Apache Airflow.

Airflow calls `run_asset(name, mode)` and this package handles everything else: extraction with rate limiting and parallelism, checkpointing for resumable retries, schema management, validation, and atomic promotion.

## Quick Start

```python
from data_assets import run_asset

result = run_asset("sonarqube_projects", run_mode="full")
```

## Built-in Sources

Production-ready assets across 5 sources, with no custom code required:

| Source | Assets | Description |
|--------|--------|-------------|
| GitHub | 12 | Repositories, pull requests, branches, commits, CI/CD workflows and runs, org members, runner groups (multi-org support) |
| ServiceNow | 13 | ITSM tables: incidents, changes, change tasks, and related operational data via pysnc |
| SonarQube | 8 | Code quality: projects, issues, branches, analyses, measures, and historical trends |
| Jira | 2 | Project tracking: projects and issues (Cloud and Data Center) |
| Transforms | 1 | SQL-based derived tables (Postgres-to-Postgres) |

See the [Assets Catalog](docs/assets-catalog.md) for the full reference.

## Key Features

- **Atomic runs** -- temp table per run, promote to production only on validation success
- **Resumable extraction** -- checkpoint-based retry without re-fetching completed pages
- **Parallel extraction** -- page-parallel and entity-parallel thread pool modes
- **Self-managing schemas** -- auto-create tables with additive column migration
- **In-process rate limiting** -- thread-safe sliding-window per source
- **Pluggable token management** -- per-source auth (GitHub App JWT, ServiceNow OAuth2, SonarQube token, Jira Cloud/Data Center)
- **RestAsset pattern** -- declarative asset definition for standard REST APIs (~25 lines, no custom code)
- **Production gate** -- new assets require explicit activation via `dag_overrides.toml` before running on a schedule
- **Dry run mode** -- extract and validate without writing to the target table
- **Stale-run takeover** -- automatic recovery from orphaned runs via heartbeat monitoring

## Documentation

1. [Local Dev Quickstart](docs/quickstart-dev.md) -- get running locally
2. [Running DAGs Locally](docs/local-airflow.md) -- install Airflow, generate DAGs, trigger runs
3. [User Guide](docs/user-guide.md) -- run modes, watermarks, runtime overrides
4. [Airflow Deployment](docs/airflow-deployment.md) -- DAG generation, systemd automation, admin overrides
5. [Architecture](docs/architecture.md) -- ETL lifecycle and component design
6. [Configuration](docs/configuration.md) -- credentials, Airflow Connections, proxy setup
7. [Assets Catalog](docs/assets-catalog.md) -- all built-in assets with API details
8. [Extending](docs/extending.md) -- adding new sources, transforms, and token managers
9. [Testing Guide](docs/testing.md) -- test structure, fixtures, and coverage

See [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow and PR guidelines.

## Requirements

- Python 3.11+
- PostgreSQL 14+
- Apache Airflow 3.0+ (for scheduling)

## License

Apache License 2.0
