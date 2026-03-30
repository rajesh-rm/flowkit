# Local Development Quickstart

Get the `data_assets` package running locally in under 5 minutes.

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.11+ | `uv python install 3.11` |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker | 20+ | [docker.com](https://www.docker.com/) (for integration tests only) |
| PostgreSQL | 14+ | Docker or local install (for integration tests only) |

## 1. Clone and enter the repo

```bash
git clone https://github.com/rajesh-rm/flowkit.git
cd flowkit
```

## 2. Set up the environment with uv

```bash
# Create virtual environment (downloads Python 3.11 if needed)
uv venv .venv --python 3.11

# Install the package in editable mode with all dev dependencies
uv pip install -e ".[dev]"
```

That's it. The `.venv` directory is gitignored.

## 3. Activate the environment

```bash
source .venv/bin/activate
```

Or prefix commands with `.venv/bin/` without activating.

## 4. Run unit tests (no Docker required)

```bash
# All unit tests — no database, no network, no Docker
pytest tests/unit/ -v

# With coverage
pytest tests/unit/ --cov=data_assets --cov-report=term-missing
```

Unit tests cover:
- Core types, enums, column definitions
- Asset base class (default transform, validation)
- Asset registry (register, lookup, decorator)
- API client (mocked HTTP via respx)
- Rate limiter (token bucket, thread safety)
- Pagination helpers
- Schema manager DDL generation
- Composable validators

## 5. Run integration tests (Docker required)

Integration tests use [testcontainers](https://testcontainers.com/) to spin up an ephemeral Postgres instance in Docker. Make sure Docker is running.

```bash
# Run all integration tests
pytest tests/integration/ -v -m integration

# Run tests for a single source
pytest tests/integration/test_e2e_sonarqube.py -v
pytest tests/integration/test_e2e_github.py -v
```

If you don't have Docker, you can point to an existing Postgres:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/test_data_assets"
pytest tests/integration/ -v -m integration
```

## 6. Run all tests

```bash
pytest -v
```

## 7. Code quality

```bash
# Lint
ruff check src/ tests/

# Auto-fix
ruff check src/ tests/ --fix

# Type checking
mypy src/data_assets/
```

## Project layout

```
flowkit/
├── src/data_assets/          # Package source (installed editable)
│   ├── core/                 # Asset classes, enums, types, registry
│   ├── extract/              # API client, rate limiter, tokens, parallel
│   ├── load/                 # Schema mgmt, temp tables, promotion
│   ├── checkpoint/           # Locks, checkpoints
│   ├── observability/        # Logging, run metrics
│   ├── db/                   # SQLAlchemy engine + ORM models
│   ├── validation/           # Composable validators
│   ├── transform/            # SQL transforms
│   ├── assets/               # Concrete asset definitions
│   │   ├── sonarqube/
│   │   ├── servicenow/
│   │   ├── github/
│   │   ├── jira/
│   │   └── transforms/
│   └── runner.py             # Main orchestrator
├── tests/
│   ├── unit/                 # Fast tests, no DB
│   ├── integration/          # E2E with Postgres + mocked APIs
│   └── fixtures/             # Sample API responses (JSON)
├── docs/                     # Documentation
├── example_dags/             # Airflow DAG examples
├── initial_architecture.md   # Architecture specification
└── pyproject.toml            # Build config + dependencies
```

## Common development workflows

### Adding a new asset

1. Create `src/data_assets/assets/my_source/my_asset.py`
2. Subclass `APIAsset` or `TransformAsset`
3. Decorate with `@register`
4. Import in `assets/my_source/__init__.py`
5. Add test fixtures in `tests/fixtures/my_source/`
6. Add unit + integration tests
7. Run `pytest -v` to verify

### Running a single asset locally

```bash
# Set credentials
export DATABASE_URL="postgresql://user:pass@localhost:5432/data_assets"
export SONARQUBE_URL="https://sonar.example.com"
export SONARQUBE_TOKEN="sqa_xxxxx"

# Run from Python
python -c "from data_assets import run_asset; print(run_asset('sonarqube_projects', 'full'))"
```

### Resetting local state

If you need to wipe test data:

```sql
-- Drop all asset data tables
DROP SCHEMA raw CASCADE; CREATE SCHEMA raw;
DROP SCHEMA mart CASCADE; CREATE SCHEMA mart;
DROP SCHEMA temp_store CASCADE; CREATE SCHEMA temp_store;

-- Clear operational metadata
TRUNCATE data_ops.run_locks, data_ops.run_history,
         data_ops.checkpoints, data_ops.asset_registry,
         data_ops.coverage_tracker;
```

## Environment variables reference

See [docs/configuration.md](configuration.md) for the full list. Minimum for local dev:

```bash
# Required for any run
export DATABASE_URL="postgresql://localhost:5432/data_assets"

# Per-source (set only what you need)
export SONARQUBE_URL="..."
export SONARQUBE_TOKEN="..."
export SERVICENOW_INSTANCE="..."
export SERVICENOW_USERNAME="..."
export SERVICENOW_PASSWORD="..."
export GITHUB_APP_ID="..."
export GITHUB_PRIVATE_KEY="..."
export GITHUB_INSTALLATION_ID="..."
export GITHUB_ORGS="org-one,org-two"
export JIRA_URL="..."
export JIRA_EMAIL="..."
export JIRA_API_TOKEN="..."
```

You can also put these in a `.env` file at the repo root (gitignored).
