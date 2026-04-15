# Tutorial: Local Development Setup

By the end of this tutorial, you will have a working local development environment with the package installed, a database running, and all tests passing.

Get the `data_assets` package running locally in under 5 minutes.

> This guide targets **RHEL 8/9** (and compatible distros: CentOS Stream, Rocky Linux, AlmaLinux). macOS and other Linux distros work with minor adjustments noted inline.

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Python | 3.11+ | RHEL AppStream (see below) or `uv python install 3.11` |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Podman | 4.0+ | Pre-installed on RHEL 8/9. macOS/other: Docker 20+ works too |
| PostgreSQL or MariaDB | PostgreSQL 16+ / MariaDB 10.11+ | Container (recommended) or system install (see section 6) |

### RHEL 8/9 system packages

```bash
# RHEL 8: enable the Python 3.11 module stream first
sudo dnf module enable python3.11 -y

# RHEL 8 and 9: install Python, build tools, and Postgres client library
sudo dnf install python3.11 python3.11-devel python3.11-pip gcc libpq-devel -y
```

- `python3.11-devel` — needed for C extensions (psycopg2 compilation fallback)
- `libpq-devel` — needed if no pre-built psycopg2-binary wheel is available for your platform
- `gcc` — required for any native extension compilation

> **macOS:** use `uv python install 3.11` instead. Xcode command-line tools provide the C compiler.

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

## 3. Container runtime (Podman)

RHEL ships Podman instead of Docker. Integration tests use [testcontainers](https://testcontainers.com/) to spin up an ephemeral Postgres, and testcontainers communicates via a Docker-compatible socket.

```bash
# Enable the Podman socket (one-time setup)
systemctl --user enable --now podman.socket

# Verify it is active
systemctl --user status podman.socket

# Tell testcontainers where to find the socket
# Add this to ~/.bashrc so it persists:
export DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock"
```

The test suite auto-detects the Podman socket and disables Ryuk (a Docker-only cleanup sidecar). If you see a log message like `"Ryuk disabled"` during integration tests, that is expected.

> **macOS:** Docker Desktop provides the socket automatically. No extra setup needed.

## 4. Set up the environment with uv

```bash
# Create virtual environment (downloads Python 3.11 if needed)
uv venv .venv --python 3.11

# Install the package in editable mode with all dev dependencies
uv pip install -e ".[dev]"
```

That's it. The `.venv` directory is gitignored.

## 5. Activate the environment

```bash
source .venv/bin/activate
```

Or prefix commands with `.venv/bin/` without activating.

## 6. Set up the Database

You need a database for running assets (not for unit tests — those run without a database). Choose **PostgreSQL** or **MariaDB**, then pick a setup option.

### PostgreSQL (container — recommended)

```bash
pip install data-assets[postgres]   # install the PostgreSQL driver

podman run -d \
  --name flowkit-postgres \
  -e POSTGRES_USER=flowkit \
  -e POSTGRES_PASSWORD=flowkit \
  -e POSTGRES_DB=data_assets \
  -p 5432:5432 \
  postgres:16-alpine

export DATABASE_URL="postgresql://flowkit:flowkit@localhost:5432/data_assets"
```

Container lifecycle:

```bash
podman stop flowkit-postgres     # Stop
podman start flowkit-postgres    # Restart
podman logs flowkit-postgres     # View logs
podman rm -f flowkit-postgres    # Remove and recreate
```

### MariaDB (container)

```bash
pip install data-assets[mariadb]   # install the MariaDB driver (PyMySQL)

podman run -d \
  --name flowkit-mariadb \
  -e MARIADB_USER=flowkit \
  -e MARIADB_PASSWORD=flowkit \
  -e MARIADB_DATABASE=data_assets \
  -e MARIADB_ROOT_PASSWORD=rootpass \
  -p 3306:3306 \
  mariadb:10.11

export DATABASE_URL="mysql+pymysql://flowkit:flowkit@localhost:3306/data_assets"
```

Container lifecycle:

```bash
podman stop flowkit-mariadb      # Stop
podman start flowkit-mariadb     # Restart
podman logs flowkit-mariadb      # View logs
podman rm -f flowkit-mariadb     # Remove and recreate
```

> **macOS:** replace `podman` with `docker` in the commands above.

### System install (RHEL — PostgreSQL)

```bash
sudo dnf install postgresql-server postgresql -y
sudo postgresql-setup --initdb
sudo systemctl enable --now postgresql
sudo -u postgres psql -c "CREATE USER flowkit WITH PASSWORD 'flowkit';"
sudo -u postgres psql -c "CREATE DATABASE data_assets OWNER flowkit;"
sudo sed -i '/^host/s/ident$/md5/' /var/lib/pgsql/data/pg_hba.conf
sudo systemctl restart postgresql

export DATABASE_URL="postgresql://flowkit:flowkit@localhost:5432/data_assets"
```

### System install (RHEL — MariaDB)

```bash
sudo dnf install mariadb-server mariadb -y
sudo systemctl enable --now mariadb
sudo mysql -e "CREATE DATABASE data_assets;"
sudo mysql -e "CREATE USER 'flowkit'@'localhost' IDENTIFIED BY 'flowkit';"
sudo mysql -e "GRANT ALL ON data_assets.* TO 'flowkit'@'localhost';"

export DATABASE_URL="mysql+pymysql://flowkit:flowkit@localhost:3306/data_assets"
```

### Verify the connection

```bash
.venv/bin/python -c "
from data_assets.db.engine import get_engine, ensure_schemas
engine = get_engine()
ensure_schemas(engine)
print('Connected:', engine.url)
"
```

> **No manual DDL required.** The package auto-creates all schemas (`data_ops`, `raw`, `mart`, `temp_store`) and metadata tables on the first `run_asset()` call.

### What happens on first run

When you call `run_asset()` for the first time against a fresh database:

1. Creates schemas: `data_ops`, `raw`, `mart`, `temp_store`
2. Creates metadata tables: `run_locks`, `run_history`, `checkpoints`, `asset_registry`, `coverage_tracker`
3. Discovers and registers all asset classes from `data_assets.assets.*`
4. Acquires a run lock for the asset
5. Extracts data from the source API into a temporary table in `temp_store`
6. Validates the extracted data
7. Promotes data to the target table in `raw` (or `mart` for transforms)
8. Records the run result in `run_history` and releases the lock

## 7. Run unit tests (no Postgres or Docker required)

```bash
# All unit tests — no database, no network, no Docker
make test-unit

# With coverage
make test-cov
```

## 8. Run integration tests (Podman/Docker required)

Integration tests use [testcontainers](https://testcontainers.com/) to spin up an ephemeral Postgres. Make sure the container runtime is set up (see section 3).

```bash
make test-integration
```

If you don't have a container runtime, point to your local Postgres instead:

```bash
export DATABASE_URL="postgresql://flowkit:flowkit@localhost:5432/data_assets"
.venv/bin/python -m pytest tests/integration/ -v -m integration
```

## 9. Run all tests

```bash
make test
```

For the full testing guide — directory structure, fixtures, patterns, and how to write tests — see [docs/testing.md](testing.md).

## 10. Code quality

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
│   ├── integration/          # E2E with real DB + mocked APIs
│   └── fixtures/             # Sample API responses (JSON)
├── docs/                     # Documentation
├── example_dags/             # Airflow DAG examples
└── pyproject.toml            # Build config + dependencies
```

## Common development workflows

### Testing DAGs locally with Airflow

See [Running DAGs Locally](local-airflow.md) for a step-by-step guide to install Airflow locally, generate DAGs from the package, and trigger runs through the Airflow UI.

### Adding a new asset

See [Tutorial: Build Your First Asset](tutorial-first-asset.md) for the step-by-step guide, or [Extending Reference](extending-reference.md) for the full attribute documentation.

**API assets** (fetch data from external APIs):

1. Create token manager in `extract/token_manager.py` (if new source)
2. Create `src/data_assets/assets/my_source/my_asset.py` — subclass `APIAsset`, add `@register`
3. Create `src/data_assets/assets/my_source/__init__.py` — import your asset class
4. Add import in `src/data_assets/assets/__init__.py`
5. Add test fixtures in `tests/fixtures/my_source/` (JSON files matching API responses)
6. Add unit test in `tests/unit/assets/test_my_source.py`
7. Run `make test-unit` to verify

**Transform assets** (SQL-based derived tables):

1. Create `src/data_assets/assets/transforms/my_transform.py` — subclass `TransformAsset`, add `@register`
2. Set `source_tables = [...]` — must match `target_table` of existing assets (enforced at discovery time)
3. Implement `query(context)` — use fully-qualified table names (e.g., `raw.servicenow_incidents`)
4. Add import in `src/data_assets/assets/transforms/__init__.py`
5. Run `.venv/bin/python -m pytest tests/unit/transforms/ -v` — validates source tables, SQL column refs, and output columns

### Debugging checklist

See [How to debug a failed run](how-to-guides.md#how-to-debug-a-failed-run) for comprehensive setup and runtime error tables.

### Running a single asset locally

```bash
export DATABASE_URL="postgresql://flowkit:flowkit@localhost:5432/data_assets"
export SONARQUBE_URL="https://sonar.example.com"
export SONARQUBE_TOKEN="sqa_xxxxx"

.venv/bin/python -c "
from data_assets import run_asset
result = run_asset('sonarqube_projects', 'full')
print(result)
# {'rows_extracted': 42, 'rows_loaded': 42, 'duration_seconds': 3.2, 'status': 'success'}
"
```

### Testing with limited data

See [How to run against a test slice of data](how-to-guides.md#how-to-run-against-a-test-slice-of-data) for using `max_pages`, `max_entities`, and `dry_run`.

### Resetting local state

See [How to reset local state](how-to-guides.md#how-to-reset-local-state) for SQL commands to wipe test data.

## Environment variables reference

See [docs/configuration.md](configuration.md) for the full list. Minimum for local dev:

```bash
# Required for any run (use the URL for your database)
export DATABASE_URL="postgresql://flowkit:flowkit@localhost:5432/data_assets"   # PostgreSQL
# export DATABASE_URL="mysql+pymysql://flowkit:flowkit@localhost:3306/data_assets"  # MariaDB

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
