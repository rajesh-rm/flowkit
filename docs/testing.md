# Testing Guide

This guide covers everything you need to write, run, and debug tests for `data_assets`.

## Quick reference

```bash
make test-unit           # Unit tests only (no Docker)
make test-integration    # Integration tests (requires Docker)
make test                # All tests
make test-cov            # Unit tests with coverage report

# Or directly with pytest:
.venv/bin/python -m pytest tests/unit/ -v                      # All unit tests
.venv/bin/python -m pytest tests/unit/assets/test_github.py -v # One file
.venv/bin/python -m pytest tests/unit/ -k "test_parse_response" # By name pattern

# Full coverage report (unit + integration, as used by SonarQube):
.venv/bin/python -m pytest tests/ --cov=src/data_assets --cov-report=xml:coverage.xml
```

**Coverage target: 90%+** (run `pytest --cov` to check current coverage).

---

## Test directory structure

```
tests/
├── conftest.py                     # Root fixtures (shared by ALL tests)
│
├── fixtures/                       # JSON API response fixtures
│   ├── github/                     #   repos, PRs, branches, workflows, ...
│   ├── jira/                       #   projects, issues
│   ├── servicenow/                 #   incidents, changes, ...
│   └── sonarqube/                  #   projects, issues, measures, branches, ...
│
├── unit/                           # Fast tests — no DB, no network, no Docker
│   ├── conftest.py                 #   make_ctx() helper + env fixtures
│   ├── assets/                     #   Asset-level tests (build_request, parse_response)
│   │   ├── test_github.py          #     all GitHub assets
│   │   ├── test_servicenow.py      #     all ServiceNow assets
│   │   ├── test_jira.py            #     Jira projects + issues
│   │   └── test_sonarqube.py       #     all SonarQube assets
│   ├── core/                       #   Framework core
│   │   ├── test_asset.py           #     Asset base class, classify_error, should_stop
│   │   ├── test_column.py          #     Column DDL generation
│   │   ├── test_enums.py           #     RunMode, LoadStrategy, etc.
│   │   ├── test_identifiers.py     #     Naming conventions
│   │   ├── test_registry.py        #     @register, discover(), get()
│   │   ├── test_rest_asset.py      #     RestAsset declarative config + pagination
│   │   └── test_types.py           #     PaginationState, RequestSpec, etc.
│   ├── extract/                    #   Extraction layer
│   │   ├── test_api_client.py      #     HTTP retries, error handling (respx)
│   │   ├── test_parallel.py        #     Parallel extraction, checkpoints, max_pages
│   │   ├── test_rate_limiter.py    #     Sliding-window rate limiter
│   │   └── test_token_manager.py   #     All token managers (GitHub, ServiceNow, Jira, SonarQube)
│   ├── runner/
│   │   └── test_runner.py          #     Orchestrator: routing, error handling, watermarks
│   ├── db/
│   │   └── test_engine.py          #     DB connection resolution, schema creation
│   ├── transform/
│   │   └── test_db_transform.py    #     SQL transform execution
│   ├── transforms/
│   │   └── test_transform_validation.py  # Transform dependency + column validation
│   └── validation/
│       └── test_validators.py      #     Composable validators (null PK, empty DF, etc.)
│
└── integration/                    # Slow tests — real DB via testcontainers (Postgres or MariaDB)
    ├── conftest.py                 #   stub_token_manager, run_engine, seed_table
    ├── test_e2e.py                 #     Full run_asset() lifecycle (API mock + DB)
    ├── test_loader.py              #     DDL, temp tables, promotion strategies
    ├── test_checkpoint.py          #     Checkpoint save/resume/recovery
    ├── test_run_tracker.py         #     Run history recording
    └── test_transform_schema.py    #     Transform SQL against empty DB tables
```

### How tests are organized

| Directory | What it tests | Needs Docker? | Mocking style |
|-----------|--------------|---------------|---------------|
| `unit/assets/` | Each asset's `build_request()` and `parse_response()` | No | `monkeypatch` for env vars, JSON fixtures for responses |
| `unit/core/` | Framework classes (Asset, Column, RestAsset, Registry, enums) | No | Direct instantiation, minimal mocking |
| `unit/extract/` | API client, rate limiter, parallel extraction, token managers | No | `respx` for HTTP, `monkeypatch` for env, `MagicMock` for complex deps |
| `unit/runner/` | Orchestrator logic (routing, error paths, watermarks) | No | Heavy `@patch` — mocks engine, DB calls, and asset methods |
| `unit/db/` | Engine factory, schema creation | No | `monkeypatch` for env vars, `MagicMock` for SQLAlchemy |
| `unit/transform/` | SQL transform execution | No | `MagicMock` for engine and `pd.read_sql` |
| `unit/transforms/` | Transform dependency + column validation | No | Registry stubs, SQL regex parsing |
| `unit/validation/` | Validator functions | No | Direct instantiation with test DataFrames |
| `integration/` | Full pipeline against real Postgres or MariaDB | **Yes** | `respx` for HTTP + testcontainers for DB |

---

## Fixtures and helpers

### Root conftest (`tests/conftest.py`)

Available in **all** tests (unit and integration):

| Fixture/Helper | Type | Description |
|----------------|------|-------------|
| `StubTokenManager` | Class | Minimal token manager — `get_token()` returns `"test-token"` |
| `_clean_registry` | Autouse fixture | Isolates the asset registry between tests (prevents `@register` leakage) |
| `db_engine` / `pg_engine` | Session fixture | Real database via testcontainers. Set `TEST_DATABASE=mariadb` for MariaDB (default: postgres) |
| `clean_db` | Function fixture | Truncates all tables before each test, returns engine |
| `load_fixture` | Function fixture | Callable: `load_fixture("github/repos_org1.json")` loads JSON from `tests/fixtures/` |

### Unit conftest (`tests/unit/conftest.py`)

Available in unit tests only:

| Fixture/Helper | Type | Description |
|----------------|------|-------------|
| `make_ctx(**kwargs)` | Function (not fixture) | Creates `RunContext` with defaults: `run_id=uuid4(), mode=FULL, asset_name="test"` |
| `github_env` | Fixture | Sets `GITHUB_APP_ID`, `GITHUB_PRIVATE_KEY`, `GITHUB_INSTALLATION_ID`, `GITHUB_ORGS` |
| `jira_env` | Fixture | Sets `JIRA_URL`, `JIRA_EMAIL`, `JIRA_API_TOKEN` |
| `sonarqube_env` | Fixture | Sets `SONARQUBE_URL`, `SONARQUBE_TOKEN` |
| `servicenow_env` | Fixture | Sets `SERVICENOW_INSTANCE`, `SERVICENOW_USERNAME`, `SERVICENOW_PASSWORD` |

### Integration conftest (`tests/integration/conftest.py`)

Available in integration tests only:

| Fixture/Helper | Type | Description |
|----------------|------|-------------|
| `stub_token_manager(cls)` | Context manager | Patches a TokenManager subclass to skip real credential resolution |
| `run_engine` | Fixture | Patches `get_engine()` everywhere to use test database |
| `seed_table(engine, schema, table, rows)` | Function | Inserts setup data into a table before test |

---

## How to write tests

### Pattern 1: Testing a new asset (most common)

When you add or modify an asset, you need two things: a fixture file and a test file.

**Step 1: Create a JSON fixture** matching a real API response.

Save it to `tests/fixtures/<source>/<endpoint>.json`:

```json
// tests/fixtures/pagerduty/incidents_page1.json
{
  "incidents": [
    {
      "id": "P123ABC",
      "title": "CPU usage critical on web-01",
      "status": "resolved",
      "created_at": "2025-06-15T08:30:00Z"
    }
  ],
  "more": false,
  "total": 1
}
```

**Step 2: Write unit tests** in `tests/unit/assets/test_<source>.py`.

Every asset needs at minimum these tests:

```python
"""Tests for PagerDuty incidents asset."""
import json
from pathlib import Path

from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "pagerduty"


class TestPagerDutyIncidentsBuildRequest:
    def test_first_page(self, pagerduty_env):
        from data_assets.assets.pagerduty.incidents import PagerDutyIncidents

        spec = PagerDutyIncidents().build_request(make_ctx(), checkpoint=None)
        assert spec.method == "GET"
        assert "/incidents" in spec.url
        assert spec.params["offset"] == 0

    def test_with_checkpoint(self, pagerduty_env):
        from data_assets.assets.pagerduty.incidents import PagerDutyIncidents

        spec = PagerDutyIncidents().build_request(
            make_ctx(), checkpoint={"next_offset": 200}
        )
        assert spec.params["offset"] == 200


class TestPagerDutyIncidentsParseResponse:
    def test_happy_path(self, pagerduty_env):
        from data_assets.assets.pagerduty.incidents import PagerDutyIncidents

        data = json.loads((FIXTURES / "incidents_page1.json").read_text())
        df, state = PagerDutyIncidents().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["id"] == "P123ABC"
        assert state.has_more is False

    def test_empty_response(self, pagerduty_env):
        from data_assets.assets.pagerduty.incidents import PagerDutyIncidents

        df, state = PagerDutyIncidents().parse_response(
            {"incidents": [], "more": False, "total": 0}
        )
        assert len(df) == 0
        assert list(df.columns) == [c.name for c in PagerDutyIncidents().columns]
```

**Key conventions:**
- Organize tests as classes: `TestAssetNameBuildRequest`, `TestAssetNameParseResponse`
- Import the asset class inside each test method (avoids import-time side effects from `@register`)
- Use the `<source>_env` fixture to set required env vars
- Test both happy path and empty response
- Verify column names match the asset's `columns` definition

**Step 3: Add an env fixture** if this is a new source. In `tests/unit/conftest.py`:

```python
@pytest.fixture
def pagerduty_env(monkeypatch):
    """Set all PagerDuty env vars needed to instantiate PagerDuty assets."""
    monkeypatch.setenv("PAGERDUTY_URL", "https://api.pagerduty.com")
    monkeypatch.setenv("PAGERDUTY_TOKEN", "test-token")
```

### Pattern 2: Testing framework internals

For modules in `core/`, `extract/`, `db/`, etc., use `@patch` and `MagicMock` to isolate the code under test from its dependencies.

```python
from unittest.mock import MagicMock, patch

class TestExtractRoute:
    @patch("data_assets.runner.execute_transform")
    @patch("data_assets.runner._check_source_freshness")
    def test_transform_asset_route(self, mock_fresh, mock_exec):
        """TransformAsset routes to execute_transform."""
        mock_exec.return_value = 42
        asset = MagicMock(spec=TransformAsset)
        asset.query.return_value = "SELECT 1"
        asset.query_timeout_seconds = 300

        rows, stats = _extract_route(asset, engine, "tmp", ctx, {}, {})
        assert rows == 42
        mock_exec.assert_called_once()
```

**When to use which mock tool:**

| Tool | Use when |
|------|----------|
| `monkeypatch.setenv()` | Setting/clearing environment variables |
| `respx` | Mocking HTTP calls made by `httpx` (API client tests) |
| `@patch("module.path.function")` | Replacing a function/class at import path (runner internals) |
| `MagicMock(spec=SomeClass)` | Creating a fake object that respects an interface |
| `MagicMock()` (no spec) | Quick mock when you don't need type safety |

### Pattern 3: Integration tests (full pipeline)

Integration tests call `run_asset()` with a real database (Postgres or MariaDB) and mocked HTTP responses.

```python
import respx
import httpx
import pytest

@pytest.mark.integration
@respx.mock
def test_full_run(run_engine, monkeypatch):
    """End-to-end: mock API → extract → load → verify in DB."""
    from tests.integration.conftest import stub_token_manager
    from data_assets.assets.sonarqube.projects import SonarQubeProjects
    from data_assets.extract.token_manager import SonarQubeTokenManager

    monkeypatch.setenv("SONARQUBE_URL", "https://sonar.test")
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")

    # Mock the API response
    respx.get("https://sonar.test/api/projects/search").mock(
        return_value=httpx.Response(200, json=fixture_data)
    )

    with stub_token_manager(SonarQubeTokenManager):
        from data_assets import run_asset
        result = run_asset("sonarqube_projects", "full")

    assert result["rows_loaded"] > 0

    # Verify data landed in the correct table
    import pandas as pd
    df = pd.read_sql("SELECT * FROM raw.sonarqube_projects", run_engine)
    assert len(df) == expected_count
```

**Integration test requirements:**
- Always use `@pytest.mark.integration` marker
- Use `run_engine` fixture (patches `get_engine()` to use test database)
- Use `stub_token_manager()` to bypass real credential resolution
- Use `respx` to mock HTTP — never make real API calls in tests
- Seed parent tables with `seed_table()` if the asset depends on a parent (entity-parallel)

---

## Test data: fixtures

Fixture files live in `tests/fixtures/<source>/` and contain real API response shapes with fake data.

### Creating fixture data

1. Make a real API call (or copy from API docs) to get the response structure
2. Replace sensitive data (names, emails, IDs) with fake values
3. Keep 2-3 records — enough to test pagination and edge cases
4. Save as `<endpoint>.json` or `<endpoint>_page1.json`

### Loading fixtures in tests

```python
# Option A: Direct path (most common in unit tests)
from pathlib import Path
FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "github"
data = json.loads((FIXTURES / "repos_org1.json").read_text())

# Option B: load_fixture helper (available as a pytest fixture)
def test_example(load_fixture):
    data = load_fixture("github/repos_org1.json")
```

### Current fixture inventory

| Source | Files | Records per file |
|--------|-------|-----------------|
| GitHub | 12 | 2-3 each (repos, PRs, branches, commits, workflows, runs, jobs, members, user details, runner groups, properties) |
| ServiceNow | 13 | 2 each (incidents, changes, change_tasks, problems, users, user_groups, departments, locations, cmdb_cis, hardware_assets, choices, catalog_items, catalog_requests) |
| Jira | 2 | 2-3 each (projects, issues) |
| SonarQube | 9 | 1-5 each (projects, issues, measures, branches, analyses, project details, measures history) + sharding fixtures |

---

## Checklist: what to test

### For every asset

- [ ] `build_request()` — first page (no checkpoint)
- [ ] `build_request()` — with checkpoint (pagination resume)
- [ ] `build_request()` — with `start_date` (incremental mode, if applicable)
- [ ] `parse_response()` — happy path with fixture data
- [ ] `parse_response()` — empty response returns correct columns
- [ ] `parse_response()` — pagination state (`has_more`, `next_page`/`next_offset`/`cursor`)
- [ ] `primary_key` — verify it's set and matches expected columns
- [ ] `indexes` — verify at least one index is defined
- [ ] `filter_entity_keys()` — if entity-parallel, verify filtering logic
- [ ] `should_stop()` — if overridden, test watermark-based early stop

### For framework changes

- [ ] Happy path works
- [ ] Error paths raise expected exceptions
- [ ] Edge cases (empty input, None values, boundary conditions)
- [ ] If adding a new `run_asset()` override: test that it propagates through `_extract_api()` to all three extraction modes
- [ ] Existing tests still pass (`make test-unit`)

### Before submitting a PR

```bash
make test-unit       # All unit tests pass
make lint            # No lint errors
make typecheck       # No type errors (optional but recommended)
```

---

## Debugging failed tests

### Common issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| `RuntimeError: GITHUB_APP_ID` | Missing env fixture | Add `github_env` (or relevant) fixture to your test |
| `KeyError` in `_registry` | Asset not registered | Ensure `@register` decorator is on the class |
| `respx.CallNotMocked` | HTTP call not mocked | Add `respx.get(url).mock(...)` for the endpoint |
| Integration test skipped | Docker not running | Start Docker Desktop / `colima start` / `systemctl start podman` |
| `AttributeError: 'MagicMock'` | Mock missing attribute | Use `MagicMock(spec=RealClass)` or set the attribute explicitly |
| Test passes alone, fails in suite | Registry leakage | The `_clean_registry` autouse fixture should handle this; check import-time side effects |

### Running a single test with verbose output

```bash
# Single test class
.venv/bin/python -m pytest tests/unit/assets/test_github.py::TestGitHubReposBuildRequest -v

# Single test method
.venv/bin/python -m pytest tests/unit/assets/test_github.py::TestGitHubReposBuildRequest::test_first_page -v

# With print output visible
.venv/bin/python -m pytest tests/unit/assets/test_github.py -v -s

# Stop on first failure
.venv/bin/python -m pytest tests/unit/ -x

# Show local variables in tracebacks
.venv/bin/python -m pytest tests/unit/ --tb=long
```

---

## Dependencies

All test dependencies are in `pyproject.toml` under `[project.optional-dependencies] dev`:

| Package | Version | Purpose |
|---------|---------|---------|
| `pytest` | >= 8.0 | Test framework |
| `pytest-cov` | latest | Coverage reporting (`--cov` flag) |
| `pytest-mock` | latest | `mocker` fixture (wrapper around `unittest.mock`) |
| `respx` | >= 0.21 | HTTP mocking for `httpx` (our HTTP client) |
| `testcontainers[postgres,mysql]` | >= 4.0 | Ephemeral database containers for integration tests |
| `ruff` | latest | Linter |
| `mypy` | latest | Type checker |

Install everything: `uv pip install -e ".[dev]"`
