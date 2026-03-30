# engmx_flowkit

Turnkey data-as-asset ETLs for Apache Airflow 3.1+.

Install the package, configure your Airflow Connections, drop a one-line stub DAG, and asset-driven ETL pipelines appear in the Airflow UI — extracting data from SonarQube, ServiceNow, GitHub, and more.

## Quick Start

```python
# dags/flowkit_dags.py
from engmx_flowkit import generate_dags

globals().update(generate_dags())
```

## Sources

| Source | Assets | Description |
|--------|--------|-------------|
| SonarQube | issues, metrics, quality_gates | Code quality data |
| ServiceNow | incidents, changes, cmdb_items | ITSM and CMDB data |
| GitHub | repositories, pull_requests, commits, actions_runs | Source control and CI/CD data |

## Documentation

- [Architecture](docs/architecture.md) — design principles and component overview
- [User Guide](docs/user-guide.md) — installation and setup walkthrough
- [Configuration](docs/configuration.md) — connections, config schema, overrides
- [Assets Catalog](docs/assets-catalog.md) — all built-in assets with schemas and parameters
- [API Reference](docs/api-reference.md) — public API surface
- [Extending](docs/extending.md) — adding new sources and custom assets

## Requirements

- Python 3.11+
- Apache Airflow 3.1+

## License

Apache License 2.0
