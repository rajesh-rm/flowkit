# data_assets

Self-contained ETL engine for data assets, backed by PostgreSQL or MariaDB, orchestrated by Apache Airflow.

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
| Transforms | 1 | SQL-based derived tables (database-to-database) |

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

### Tutorials (learning-oriented)
- [Dev Environment Setup](docs/tutorial-dev-setup.md) -- clone, install, run tests
- [Build Your First Asset](docs/tutorial-first-asset.md) -- create, test, register a new asset

### How-To Guides (task-oriented)
- [How-To Guides](docs/how-to-guides.md) -- debug runs, test with limited data, multi-org, monitoring
- [Running DAGs Locally](docs/local-airflow.md) -- install Airflow, generate and trigger DAGs
- [Airflow Deployment](docs/airflow-deployment.md) -- production DAGs, systemd automation, admin overrides

### Reference (information-oriented)
- [Configuration](docs/configuration.md) -- credentials, proxy, database, runtime overrides
- [Assets Catalog](docs/assets-catalog.md) -- all built-in assets with API details
- [Extending Reference](docs/extending-reference.md) -- asset attributes, token managers, base classes
- [Testing Guide](docs/testing.md) -- test structure, fixtures, and coverage

### Explanation (understanding-oriented)
- [Architecture](docs/architecture.md) -- ETL lifecycle, run modes, component design

See [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow and PR guidelines.

## Requirements

- Python 3.11+
- PostgreSQL 16+ or MariaDB 10.11+
- Apache Airflow 3.0+ (for scheduling)

## License

Apache License 2.0
