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

## 2. Enterprise proxy setup (corporate networks only)

> Skip this section if you are on a direct internet connection.

If your network routes traffic through a corporate proxy or an internal PyPI mirror, configure these **before** running `uv` or `pip`.

### 2a. HTTP/HTTPS proxy

Set the standard proxy environment variables. Add these to your shell profile (`~/.zshrc`, `~/.bashrc`) so they persist across sessions:

```bash
export HTTP_PROXY="http://proxy.corp.example.com:8080"
export HTTPS_PROXY="http://proxy.corp.example.com:8080"
export NO_PROXY="localhost,127.0.0.1,.corp.example.com"
```

These are respected by `uv`, `pip`, `curl`, `git`, and `httpx` (the HTTP client used by data_assets at runtime).

### 2b. Internal PyPI index (Artifactory / Nexus / DevPI)

If your organization hosts an internal package mirror, configure uv to use it. Create or edit `~/.config/uv/uv.toml`:

```toml
[pip]
index-url = "https://artifactory.corp.example.com/api/pypi/pypi-remote/simple"
# Add extra indexes if you publish internal packages alongside public ones:
# extra-index-url = "https://artifactory.corp.example.com/api/pypi/pypi-internal/simple"
```

Alternatively, set it as an environment variable:

```bash
export UV_INDEX_URL="https://artifactory.corp.example.com/api/pypi/pypi-remote/simple"
```

For plain pip (used inside venvs when uv is not available):

```bash
export PIP_INDEX_URL="https://artifactory.corp.example.com/api/pypi/pypi-remote/simple"
```

Or create `~/.pip/pip.conf` (macOS/Linux) / `%APPDATA%\pip\pip.ini` (Windows):

```ini
[global]
index-url = https://artifactory.corp.example.com/api/pypi/pypi-remote/simple
```

### 2c. Custom CA certificates

If your proxy uses a corporate root CA for TLS inspection, tell uv and pip where to find the CA bundle:

```bash
# Point to your corporate CA bundle
export SSL_CERT_FILE="/etc/pki/tls/certs/corporate-ca-bundle.pem"
export REQUESTS_CA_BUNDLE="$SSL_CERT_FILE"

# For uv specifically (if the above isn't picked up):
export UV_NATIVE_TLS=true
```

If the internal PyPI index uses a certificate signed by a corporate CA that's already trusted by your OS certificate store, `UV_NATIVE_TLS=true` tells uv to use the system trust store instead of its bundled certificates.

For pip:

```bash
export PIP_CERT="/etc/pki/tls/certs/corporate-ca-bundle.pem"
```

### 2d. Verify proxy configuration

Before proceeding, verify connectivity to PyPI (or your internal mirror):

```bash
# Should return package metadata, not a proxy error
uv pip search requests 2>/dev/null || uv pip install --dry-run requests
```

## 3. Set up the environment with uv

```bash
# Create virtual environment (downloads Python 3.11 if needed)
uv venv .venv --python 3.11

# Install the package in editable mode with all dev dependencies
uv pip install -e ".[dev]"
```

That's it. The `.venv` directory is gitignored.

## 4. Activate the environment

```bash
source .venv/bin/activate
```

Or prefix commands with `.venv/bin/` without activating.

## 5. Run unit tests (no Docker required)

```bash
# All unit tests — no database, no network, no Docker
make test-unit

# With coverage
make test-cov
```

## 6. Run integration tests (Docker required)

Integration tests use [testcontainers](https://testcontainers.com/) to spin up an ephemeral Postgres instance in Docker. Make sure Docker is running.

```bash
make test-integration
```

If you don't have Docker, you can point to an existing Postgres:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/test_data_assets"
.venv/bin/python -m pytest tests/integration/ -v -m integration
```

## 7. Run all tests

```bash
make test
```

For the full testing guide — directory structure, fixtures, patterns, and how to write tests — see [docs/testing.md](testing.md).

## 8. Code quality

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
│   ├── load/                 # DDL, temp tables, promotion (loader.py)
│   ├── checkpoint/           # Locks, checkpoints
│   ├── observability/        # Logging, run tracking
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
└── pyproject.toml            # Build config + dependencies
```

## Common development workflows

### Adding a new asset

See [docs/extending.md](extending.md) for the comprehensive step-by-step guide. Quick summary:

1. Create token manager in `extract/token_manager.py` (if new source)
2. Create `src/data_assets/assets/my_source/my_asset.py` — subclass `APIAsset`, add `@register`
3. Create `src/data_assets/assets/my_source/__init__.py` — import your asset class
4. Add import in `src/data_assets/assets/__init__.py`
5. Add test fixtures in `tests/fixtures/my_source/` (JSON files matching API responses)
6. Add unit test in `tests/unit/assets/test_my_source.py`
7. Run `make test-unit` to verify

### Debugging checklist

| Symptom | Likely cause |
|---------|-------------|
| Asset not found in registry | Missing `@register` decorator or missing import in `__init__.py` |
| `build_request` never called | Entity-parallel assets use `build_entity_request` — no need to implement `build_request` |
| API returns errors | `base_url` is empty — make sure env var is set and read at runtime in `build_request` |
| Data missing from table | Column names in `parse_response` DataFrame don't match `columns` list |
| Duplicate rows | Check `primary_key` is set correctly, use `UPSERT` load strategy |
| Lock error on retry | Previous run's lock wasn't released — auto-clears after `stale_heartbeat_minutes` (default 20 min) or `max_run_hours` (default 5 hours). Override these on your asset class for slow APIs. |
| `uv pip install` fails with SSL/certificate error | Corporate proxy doing TLS inspection — set `SSL_CERT_FILE` and `UV_NATIVE_TLS=true` (see section 2c) |
| `uv pip install` fails with timeout/connection refused | Proxy not configured — set `HTTPS_PROXY` (see section 2a) |
| `pip install` downloads from wrong index | Internal mirror not configured — set `UV_INDEX_URL` or `PIP_INDEX_URL` (see section 2b) |
| `LockError: Asset 'X' is locked by run ...` | Previous run still active or crashed — wait for `stale_heartbeat_minutes` (default 20) or delete the row from `data_ops.run_locks` manually |
| `RuntimeError: Checkpoint rejected` | Another worker took over your run (stale-run takeover). Normal recovery — retry the task. |
| Asset runs for hours locally | Use `max_pages=3, dry_run=True` to validate the flow against a small slice of real data — see "Testing with limited data" above |

### Running a single asset locally

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/data_assets"
export SONARQUBE_URL="https://sonar.example.com"
export SONARQUBE_TOKEN="sqa_xxxxx"
```

```python
from data_assets import run_asset

result = run_asset("sonarqube_projects", "full")
print(result)
# {'rows_extracted': 42, 'rows_loaded': 42, 'duration_seconds': 3.2, 'status': 'success'}
```

### Testing with limited data

Assets like `github_prs` or `servicenow_incidents` can take hours to run in full against a real org. Use `max_pages` to fetch a small slice of data and validate the flow without waiting:

```python
from data_assets import run_asset

# Fetch at most 3 pages — then stop.  dry_run skips the DB write.
result = run_asset("github_prs", run_mode="full", max_pages=3, dry_run=True)
print(result)
# {'rows_extracted': 300, 'rows_loaded': 0, 'status': 'success'}
```

`max_pages` works across all extraction modes:

| Mode | What `max_pages=3` means |
|------|--------------------------|
| Sequential | Stop after 3 API calls |
| Page-parallel (e.g., Jira issues) | Fetch pages 1–3 in total, then stop |
| Entity-parallel (e.g., GitHub PRs per repo) | Each repo gets at most 3 pages |
| ServiceNow | Stop after 3 batches of 1,000 records |
| SonarQube Projects | Stop after 3 pages per shard |

> **Note:** `max_pages` is a developer testing tool. Do not set it in production DAGs — partial data will overwrite the full dataset when using `FULL_REPLACE` load strategy, and can leave `UPSERT` tables incomplete.

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
