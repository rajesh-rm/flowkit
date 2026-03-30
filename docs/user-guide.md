# User Guide

## Prerequisites

- Python 3.11+
- PostgreSQL 14+
- Apache Airflow 2.x+ (for DAG scheduling)

## Installation

```bash
pip install data-assets

# Or with dev dependencies:
pip install data-assets[dev]
```

## Quick Start

### 1. Set up Postgres

Create a database and ensure the connecting user has `CREATE SCHEMA` privileges:

```sql
CREATE DATABASE data_assets;
```

The package auto-creates the required schemas (`data_ops`, `raw`, `mart`, `temp_store`) and all metadata tables on first run.

### 2. Configure credentials

Set environment variables for your database and sources:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/data_assets"
export SONARQUBE_URL="https://sonar.example.com"
export SONARQUBE_TOKEN="sqa_xxxxx"
```

See [configuration.md](configuration.md) for all source variables.

### 3. Run an asset

```python
from data_assets import run_asset

result = run_asset("sonarqube_projects", run_mode="full")
print(result)
# {'run_id': '...', 'rows_extracted': 42, 'rows_loaded': 42, 'duration_seconds': 3.2, 'status': 'success'}
```

### 4. Deploy with Airflow

Copy `example_dags/flowkit_dags.py` to your Airflow DAGs folder. Each asset gets its own DAG with sensible defaults.

Or use the DAG factory for auto-discovery:

```python
# dags/data_assets_all.py
from example_dags.dag_factory import create_dags
globals().update(create_dags())
```

## Run Modes

| Mode | When to use |
|------|-------------|
| `full` | Initial load or periodic full refresh |
| `forward` | Incremental — fetch new data since last run |
| `backfill` | Fill in historical data going backwards |
| `transform` | Run SQL transforms (Postgres-to-Postgres) |

## Monitoring

- **Airflow UI**: Each asset is a separate DAG with tags by source
- **Run history**: Query `data_ops.run_history` for run metrics
- **Coverage**: Query `data_ops.coverage_tracker` to see watermarks
- **Logs**: All output goes to stdout (captured by Airflow task logs)
