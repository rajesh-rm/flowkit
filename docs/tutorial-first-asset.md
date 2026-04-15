# Tutorial: Build Your First Asset

By the end of this tutorial, you will have run an existing asset against a real database, built a new API asset from scratch, written tests for it, and registered it with the framework. You will also learn the RestAsset declarative pattern and the TransformAsset SQL pattern.

> **Prerequisites:** Complete the [Dev Environment Setup](tutorial-dev-setup.md) tutorial first — you need a working `.venv`, a database, and source credentials configured.

---

## Table of Contents

1. [Run an Existing Asset](#1-run-an-existing-asset)
2. [Understand the Decision Flowchart](#2-understand-the-decision-flowchart)
3. [Build a RestAsset (Declarative)](#3-build-a-restasset-declarative)
4. [Build a Custom APIAsset (Step-by-Step)](#4-build-a-custom-apiasset-step-by-step)
5. [Build a Transform Asset](#5-build-a-transform-asset)
6. [Write Tests for Your Asset](#6-write-tests-for-your-asset)
7. [Register and Verify](#7-register-and-verify)
8. [Next Steps](#8-next-steps)

---

## 1. Run an Existing Asset

Before building a new asset, run one of the built-in assets to see the full lifecycle in action.

### 1a. Set up the Database

Create a database and ensure the connecting user has `CREATE SCHEMA` privileges:

```sql
CREATE DATABASE data_assets;
```

The package auto-creates the required schemas (`data_ops`, `raw`, `mart`, `temp_store`) and all metadata tables on first run.

### 1b. Configure credentials

Set environment variables for your database and sources:

```bash
export DATABASE_URL="postgresql://user:pass@localhost:5432/data_assets"
export SONARQUBE_URL="https://sonar.example.com"
export SONARQUBE_TOKEN="sqa_xxxxx"
```

See [configuration.md](configuration.md) for all source variables.

### 1c. Run an asset

```python
from data_assets import run_asset

result = run_asset("sonarqube_projects", run_mode="full")
print(result)
# {'run_id': '...', 'rows_extracted': 42, 'rows_loaded': 42, 'duration_seconds': 3.2, 'status': 'success'}
```

### 1d. Deploy with Airflow

Use the built-in CLI to generate DAG files for all registered assets:

```bash
# List all available assets
data-assets list

# Generate DAG files in your Airflow DAGs folder
data-assets sync --output-dir /opt/airflow/dags/data_assets/
```

This creates one DAG file per asset with sensible defaults (schedule, retries, tags). To customise schedules or use Airflow Connections for secrets, create a `dag_overrides.toml` file in the same directory.

For automated zero-touch updates (new package versions automatically generate new DAGs), see the [Airflow Deployment Guide](airflow-deployment.md).

**Passing secrets explicitly** (for remote workers or KubernetesExecutor):

```python
from airflow.sdk import BaseHook
from data_assets import run_asset

conn = BaseHook.get_connection("sonarqube")
run_asset("sonarqube_projects", secrets={
    "SONARQUBE_URL": f"https://{conn.host}",
    "SONARQUBE_TOKEN": conn.password,
})
```

See [configuration.md](configuration.md) for connection setup commands.

---

## 2. Understand the Decision Flowchart

Before you write any code, answer four questions about your data source. Walk through the tree below from top to bottom.

```
QUESTION 1: Where does the data come from?
  |
  +-- External HTTP API (PagerDuty, GitHub, Jira, etc.)
  |     --> Continue to Question 1b.
  |
  +-- Existing database tables (aggregate, join, reshape)
        --> You need a TransformAsset.  Skip to Section 5.


QUESTION 1b: Is this a standard REST API? (JSON response, pagination, field mapping)
  |
  +-- YES, standard pattern (most APIs)
  |     --> Use RestAsset (declarative, ~25 lines, no code to write).
  |         See Section 3 below.
  |
  +-- The source has an official Python SDK (e.g., pysnc for ServiceNow)
  |     --> Override extract() on your asset class.
  |         See the extract() hook in the Extending Reference.
  |
  +-- NO, needs custom request/response logic
        --> Use APIAsset (custom, full control).
            Continue to Question 2.


QUESTION 2: What kind of API call is it?
  |
  +-- It lists a collection (all projects, all repos, all users)
  |     --> Sequential pagination (NONE) or PAGE_PARALLEL.
  |         Use PAGE_PARALLEL when the first response tells you the
  |         total number of pages so the framework can fetch them
  |         concurrently.
  |
  +-- It fetches child data for each parent entity
  |   (issues per project, commits per repo, alerts per service)
        --> ENTITY_PARALLEL.
            You will also need a parent asset whose primary-key values
            become the entity_keys for fan-out.


QUESTION 3: How should the data be loaded into the database?
  |
  +-- Fetch everything every run; replace the whole table.
  |     --> LoadStrategy.FULL_REPLACE
  |
  +-- Fetch new/changed data; merge by primary key.
  |     --> LoadStrategy.UPSERT
  |
  +-- Append-only (event logs, audit trails). Never update old rows.
        --> LoadStrategy.APPEND


QUESTION 4: What is the default run mode?
  |
  +-- Always fetch the full dataset.
  |     --> RunMode.FULL
  |
  +-- Normally fetch only data since the last run.
  |     --> RunMode.FORWARD
  |
  +-- Need to backfill historical data from before the first run.
        --> RunMode.BACKFILL  (typically triggered manually)
```

With those four answers in hand, you know exactly which classes and attributes you need. The rest of this tutorial shows you how to implement them.

---

## 3. Build a RestAsset (Declarative)

For the 80% of assets that follow a standard REST API pattern — fetch JSON, paginate, map fields to columns — **RestAsset eliminates the need to write `build_request()` and `parse_response()` entirely.** You just declare the endpoint, pagination, and field mapping as class attributes.

**Use RestAsset when:** the API returns JSON, uses standard pagination (page number, offset, or cursor), and you just need to extract fields from the response.

**Use APIAsset instead when:** you need custom request logic (multi-endpoint iteration, computed query parameters like JQL, keyset pagination with composite keys).

### Real example: SonarQube Projects

> **Note:** `SonarQubeProjects` extends `RestAsset` for its declarative config
> (endpoint, pagination, response_path) but overrides `extract()` to handle
> SonarQube's 10,000-result Elasticsearch limit. For instances with ≤9,900
> projects the `extract()` override uses standard pagination. For larger
> instances it shards queries using the `q` (name-substring) parameter.
> The code below shows the declarative config — see `sonarqube/projects.py`
> for the full sharding implementation.

```python
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.rest_asset import RestAsset
from data_assets.extract.token_manager import SonarQubeTokenManager


@register
class SonarQubeProjects(RestAsset):
    name = "sonarqube_projects"
    source_name = "sonarqube"
    target_schema = "raw"
    target_table = "sonarqube_projects"

    # Source config — RestAsset reads base URL from this env var at runtime
    token_manager_class = SonarQubeTokenManager
    base_url_env = "SONARQUBE_URL"
    endpoint = "/api/components/search"
    rate_limit_per_second = 5.0

    # Response parsing — RestAsset handles this automatically
    response_path = "components"          # JSON path to the records array
    pagination = {
        "strategy": "page_number",        # offset, cursor, or none also supported
        "page_size": 100,
        "total_path": "paging.total",     # JSON path to the total count
        "page_index_path": "paging.pageIndex",
    }

    # extract() handles its own pagination/sharding; the standard parallel
    # dispatch in the runner is bypassed.
    parallel_mode = ParallelMode.NONE
    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    # Schema — matches /api/components/search response fields
    columns = [
        Column("key", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("qualifier", "TEXT"),
    ]
    primary_key = ["key"]

    def build_request(self, context, checkpoint=None):
        spec = super().build_request(context, checkpoint)
        spec.params["qualifiers"] = "TRK"  # Filter to projects only
        return spec

    def extract(self, engine, temp_table, context):
        # Custom extraction: probes total, shards if >9,900.
        # See sonarqube/projects.py for full implementation.
        ...
```

RestAsset generates `build_request()` and `parse_response()` from the class attributes. The `extract()` override reuses both internally while adding sharding logic on top.

For the full RestAsset attributes reference, see [extending-reference.md](extending-reference.md).

---

## 4. Build a Custom APIAsset (Step-by-Step)

This section walks through a complete "hello world" asset. It fetches from a fictional `/api/items` endpoint, uses sequential offset pagination, and stores three columns.

### 4a. Token Manager

Add to `src/data_assets/extract/token_manager.py`:

```python
class ItemsApiTokenManager(TokenManager):
    """Static token for the fictional Items API.

    Requires: ITEMS_API_TOKEN
    """

    def __init__(self) -> None:
        super().__init__()
        self._token = _resolver.resolve("ITEMS_API_TOKEN") or ""
        if not self._token:
            raise RuntimeError("ItemsApiTokenManager requires ITEMS_API_TOKEN")

    def get_token(self) -> str:
        return self._token

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}
```

### 4b. Asset Class

Create `src/data_assets/assets/items_api/__init__.py`:

```python
"""Items API assets."""
from data_assets.assets.items_api.items import ItemsApiItems
```

Create `src/data_assets/assets/items_api/items.py`:

```python
from __future__ import annotations

import os
from typing import Any

import pandas as pd

from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import ItemsApiTokenManager


@register
class ItemsApiItems(APIAsset):
    name = "items_api_items"
    description = "All items from the Items API"
    source_name = "items_api"

    target_schema = "raw"
    target_table = "items_api_items"

    token_manager_class = ItemsApiTokenManager
    base_url = ""

    rate_limit_per_second = 10.0
    pagination_config = PaginationConfig(strategy="offset", page_size=50)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("name", "TEXT"),
        Column("created_at", "TIMESTAMPTZ"),
    ]

    primary_key = ["id"]
    date_column = "created_at"

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict | None = None,
    ) -> RequestSpec:
        base = os.environ.get("ITEMS_API_URL", "https://items.example.com")
        offset = checkpoint.get("next_offset", 0) if checkpoint else 0

        params: dict[str, Any] = {
            "offset": offset,
            "limit": self.pagination_config.page_size,
        }
        if context.start_date:
            params["since"] = context.start_date.isoformat()

        return RequestSpec(
            method="GET",
            url=f"{base}/api/items",
            params=params,
        )

    def parse_response(
        self,
        response: dict[str, Any],
    ) -> tuple[pd.DataFrame, PaginationState]:
        items = response.get("items", [])

        records = [
            {
                "id": item["id"],
                "name": item["name"],
                "created_at": item["created_at"],
            }
            for item in items
        ]

        df = pd.DataFrame(records, columns=[c.name for c in self.columns])

        total = response.get("total", 0)
        offset = response.get("offset", 0)
        fetched = len(items)
        has_more = (offset + fetched) < total

        return df, PaginationState(
            has_more=has_more,
            next_offset=offset + fetched,
            total_records=total,
        )
```

### 4c. Register the Package

Add this line to `src/data_assets/assets/__init__.py`:

```python
import data_assets.assets.items_api  # noqa: F401
```

### 4d. Set Environment Variables

```bash
export ITEMS_API_TOKEN="your-api-token-here"
export ITEMS_API_URL="https://items.example.com"
```

---

## 5. Build a Transform Asset

Transform assets produce derived data from existing database tables. They do not call any external API. Instead, they run a SQL query against the database and write the results to a new table.

**When to use:** You already have raw data in the database (from API assets) and you want to create aggregated, joined, or reshaped views of that data.

**The base class:**

```python
class TransformAsset(Asset):
    asset_type = AssetType.TRANSFORM
    default_run_mode = RunMode.TRANSFORM
    load_strategy = LoadStrategy.FULL_REPLACE
    target_schema = "mart"       # convention: transforms go in "mart" schema
    source_tables: list[str] = []
    query_timeout_seconds: int = 300  # safety limit — per-query timeout

    @abstractmethod
    def query(self, context: RunContext) -> str:
        """Return a SQL SELECT producing the output rows."""
        ...
```

The `query_timeout_seconds` attribute sets a per-query `statement_timeout` on the database session. If your transform runs a heavy multi-table JOIN that legitimately needs more than 5 minutes, override it:

```python
class HeavyTransform(TransformAsset):
    query_timeout_seconds = 900  # 15 minutes
```

### Complete example: daily incident summary

Create `src/data_assets/assets/transforms/pagerduty_incident_summary.py`:

```python
from __future__ import annotations

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.transform_asset import TransformAsset


@register
class PagerDutyIncidentSummary(TransformAsset):
    """Daily incident counts by service and urgency, from raw PagerDuty data."""

    name = "pagerduty_incident_summary"
    description = "Daily incident counts aggregated from raw PagerDuty incidents"

    target_schema = "mart"
    target_table = "pagerduty_incident_summary"

    source_tables = ["pagerduty_incidents"]
    # source_tables documents which raw tables this transform depends on.
    # The framework uses it for freshness checks and dependency ordering.

    default_run_mode = RunMode.TRANSFORM
    load_strategy = LoadStrategy.FULL_REPLACE
    # Transforms almost always use FULL_REPLACE because they re-derive
    # the entire output from the current state of the source tables.

    columns = [
        Column("report_date", "DATE", nullable=False),
        Column("service_name", "TEXT", nullable=False),
        Column("urgency", "TEXT", nullable=False),
        Column("incident_count", "INTEGER", nullable=False),
        Column("avg_resolve_hours", "FLOAT", nullable=True),
    ]

    primary_key = ["report_date", "service_name", "urgency"]

    indexes = [
        Index(columns=("report_date",)),     # filter by date range
        Index(columns=("service_name",)),    # aggregate by service
    ]

    def query(self, context: RunContext) -> str:
        """SQL SELECT that produces the summary rows.

        The column names in the SELECT must match the `columns` definition.
        """
        return """
            SELECT
                DATE(created_at)                        AS report_date,
                COALESCE(service_name, 'Unknown')       AS service_name,
                COALESCE(urgency, 'Unknown')            AS urgency,
                COUNT(*)                                AS incident_count,
                AVG(
                    EXTRACT(EPOCH FROM (resolved_at - created_at)) / 3600.0
                )                                       AS avg_resolve_hours
            FROM raw.pagerduty_incidents
            WHERE created_at IS NOT NULL
            GROUP BY DATE(created_at), service_name, urgency
            ORDER BY report_date DESC, service_name, urgency
        """
```

**The optional `transform(df)` hook:**

After the SQL query runs, the framework calls `transform(df)` on the result DataFrame. The default implementation returns `df` unchanged. Override it if you need pandas-level post-processing that is hard to express in SQL:

```python
def transform(self, df: pd.DataFrame) -> pd.DataFrame:
    # Example: add a computed column
    df["is_slow_resolution"] = df["avg_resolve_hours"] > 24.0
    return df
```

**Wiring up a transform:**

Transforms go in `src/data_assets/assets/transforms/`. The existing `__init__.py` imports transform classes. Add your new import:

```python
# src/data_assets/assets/transforms/__init__.py
from data_assets.assets.transforms.incident_summary import IncidentSummary
from data_assets.assets.transforms.pagerduty_incident_summary import PagerDutyIncidentSummary
```

No other wiring is needed — the `transforms` package is already imported by the top-level `assets/__init__.py`.

**Validation rules (enforced by CI):**

The following rules are enforced at discovery time and by unit tests. Your transform will fail CI if any are violated:

1. **`source_tables` must match registered assets.** Every entry in `source_tables` must match the `target_table` of a registered asset. If not, the registry raises a `ValueError` at import time. This prevents deploying a transform whose source data doesn't exist.

2. **SQL must use fully-qualified table names.** Write `raw.servicenow_incidents`, not just `servicenow_incidents`. The validation tests check that `FROM` and `JOIN` references match `target_schema.target_table` of registered assets.

3. **SQL column references must exist in the source asset.** If you write `WHERE opened_at IS NOT NULL`, the column `opened_at` must be declared in the source asset's `columns` list. The tests cross-reference your SQL against the source asset definitions.

4. **SELECT aliases must match declared columns.** The output columns of your `query()` must match the names and order in your `columns` list. Use `AS alias` to ensure the names align.

5. **No circular dependencies.** Transform A cannot depend on Transform B if B depends on A. The registry detects cycles and raises a `ValueError`.

Run the validation tests locally before opening a PR:

```bash
# Unit tests (fast, no DB)
.venv/bin/python -m pytest tests/unit/transforms/ -v

# Integration tests (requires Postgres via testcontainers)
.venv/bin/python -m pytest tests/integration/test_transform_schema.py -v
```

The integration test creates empty source tables from asset definitions and runs your `query()` against them — the database itself validates the SQL syntax and column references.

---

## 6. Write Tests for Your Asset

> For the full testing guide (directory structure, all fixtures, mocking patterns,
> debugging tips, and checklists), see [testing.md](testing.md).
> This section covers the minimum needed to test a new asset.

### Test Fixtures

Create a JSON file that matches the shape of a real API response. Save it in your test directory (e.g., `tests/fixtures/pagerduty/incidents_page1.json`):

```json
{
  "incidents": [
    {
      "id": "P123ABC",
      "incident_number": 42,
      "title": "CPU usage critical on web-01",
      "status": "resolved",
      "urgency": "high",
      "priority": {"summary": "P1"},
      "service": {"id": "SVC001", "summary": "Web Cluster"},
      "created_at": "2025-06-15T08:30:00Z",
      "last_status_change_at": "2025-06-15T10:15:00Z",
      "resolved_at": "2025-06-15T10:15:00Z",
      "html_url": "https://mycompany.pagerduty.com/incidents/P123ABC"
    }
  ],
  "limit": 100,
  "offset": 0,
  "total": 1,
  "more": false
}
```

### Unit Test Pattern

Instantiate the asset, call `build_request()` and `parse_response()` directly with test data. No HTTP calls, no database.

```python
# tests/test_pagerduty_incidents.py
import json
from pathlib import Path
from uuid import uuid4

import pytest

from data_assets.assets.pagerduty.incidents import PagerDutyIncidents
from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext


@pytest.fixture
def asset():
    return PagerDutyIncidents()


@pytest.fixture
def context():
    return RunContext(
        run_id=uuid4(),
        mode=RunMode.FULL,
        asset_name="pagerduty_incidents",
    )


@pytest.fixture
def sample_response():
    fixture_path = Path(__file__).parent / "fixtures" / "pagerduty" / "incidents_page1.json"
    return json.loads(fixture_path.read_text())


def test_build_request_first_page(asset, context, monkeypatch):
    monkeypatch.setenv("PAGERDUTY_URL", "https://api.pagerduty.com")
    spec = asset.build_request(context, checkpoint=None)
    assert spec.method == "GET"
    assert "/incidents" in spec.url
    assert spec.params["offset"] == 0
    assert spec.params["limit"] == 100


def test_build_request_with_checkpoint(asset, context, monkeypatch):
    monkeypatch.setenv("PAGERDUTY_URL", "https://api.pagerduty.com")
    spec = asset.build_request(context, checkpoint={"next_offset": 200})
    assert spec.params["offset"] == 200


def test_parse_response(asset, sample_response):
    df, state = asset.parse_response(sample_response)
    assert len(df) == 1
    assert df.iloc[0]["id"] == "P123ABC"
    assert df.iloc[0]["service_name"] == "Web Cluster"
    assert state.has_more is False
    assert state.total_records == 1


def test_parse_response_empty(asset):
    df, state = asset.parse_response({"incidents": [], "more": False, "total": 0})
    assert len(df) == 0
    assert list(df.columns) == [c.name for c in asset.columns]
    assert state.has_more is False
```

### Integration Test Pattern

Use `respx` to mock the HTTP layer and a test database to verify end-to-end behavior:

```python
# tests/integration/test_pagerduty_integration.py
import respx
import httpx

from data_assets.assets.pagerduty.incidents import PagerDutyIncidents


@respx.mock
def test_full_extraction(monkeypatch, sample_response):
    monkeypatch.setenv("PAGERDUTY_URL", "https://api.pagerduty.com")
    monkeypatch.setenv("PAGERDUTY_TOKEN", "test-token")

    respx.get("https://api.pagerduty.com/incidents").mock(
        return_value=httpx.Response(200, json=sample_response)
    )

    asset = PagerDutyIncidents()
    # ... run the asset through the framework and assert table contents
```

### Minimal test for the Items API example (Section 4)

```python
# tests/test_items_api.py
from uuid import uuid4

from data_assets.assets.items_api.items import ItemsApiItems
from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext


def test_build_request_defaults(monkeypatch):
    monkeypatch.setenv("ITEMS_API_URL", "https://items.example.com")
    asset = ItemsApiItems()
    ctx = RunContext(run_id=uuid4(), mode=RunMode.FULL, asset_name="items_api_items")
    spec = asset.build_request(ctx)
    assert spec.url == "https://items.example.com/api/items"
    assert spec.params["offset"] == 0


def test_parse_response_basic():
    asset = ItemsApiItems()
    response = {
        "items": [
            {"id": 1, "name": "Widget", "created_at": "2025-01-01T00:00:00Z"},
            {"id": 2, "name": "Gadget", "created_at": "2025-01-02T00:00:00Z"},
        ],
        "total": 2,
        "offset": 0,
    }
    df, state = asset.parse_response(response)
    assert len(df) == 2
    assert list(df.columns) == ["id", "name", "created_at"]
    assert state.has_more is False


def test_parse_response_pagination():
    asset = ItemsApiItems()
    response = {
        "items": [{"id": i, "name": f"Item {i}", "created_at": "2025-01-01T00:00:00Z"} for i in range(50)],
        "total": 120,
        "offset": 0,
    }
    df, state = asset.parse_response(response)
    assert len(df) == 50
    assert state.has_more is True
    assert state.next_offset == 50
```

---

## 7. Register and Verify

You have a token manager and an asset class. Here is how to make them discoverable by the framework.

**Step 1: Create the directory structure.**

```
src/data_assets/assets/pagerduty/
    __init__.py
    incidents.py
```

**Step 2: Write `__init__.py` to import the asset class.**

```python
# src/data_assets/assets/pagerduty/__init__.py
"""PagerDuty assets: incidents."""
from data_assets.assets.pagerduty.incidents import PagerDutyIncidents
```

This import is essential. The framework discovers assets by walking all subpackages under `data_assets.assets` and importing them. Importing a module that contains a `@register`-decorated class triggers the registration.

**Step 3: Add the package import to the top-level `assets/__init__.py`.**

```python
# src/data_assets/assets/__init__.py
"""Concrete asset definitions for all supported SDLC data sources."""

import data_assets.assets.sonarqube     # noqa: F401
import data_assets.assets.servicenow    # noqa: F401
import data_assets.assets.github        # noqa: F401
import data_assets.assets.jira          # noqa: F401
import data_assets.assets.transforms    # noqa: F401
import data_assets.assets.pagerduty     # noqa: F401   # <-- ADD THIS LINE
```

**Step 4: Add the token manager.**

Add your `PagerDutyTokenManager` class to `src/data_assets/extract/token_manager.py` (see Section 4a for the Items API example, or the [Extending Reference](extending-reference.md) for all four auth patterns).

**How the `@register` decorator works:**

```python
# From src/data_assets/core/registry.py:
_registry: dict[str, type[Asset]] = {}

def register(asset_cls: type[Asset]) -> type[Asset]:
    name = asset_cls.name
    _registry[name] = asset_cls
    return asset_cls
```

It is a simple class decorator. When Python imports a module and encounters `@register` on a class definition, it calls `register(PagerDutyIncidents)`, which adds it to the global `_registry` dict keyed by `asset_cls.name`. Later, the framework calls `registry.get("pagerduty_incidents")` to retrieve it.

**Checklist before you test:**

- [ ] Token manager class exists in `extract/token_manager.py`
- [ ] Asset file exists at `assets/pagerduty/incidents.py`
- [ ] Asset class has `@register` decorator
- [ ] Asset class `name` is unique across all assets
- [ ] `assets/pagerduty/__init__.py` imports the asset class
- [ ] `assets/__init__.py` imports `data_assets.assets.pagerduty`
- [ ] Required env vars are documented / set: `PAGERDUTY_TOKEN`, `PAGERDUTY_URL`

> **Production activation**: Registering a new asset does **not** automatically
> run it in production. When `data-assets sync` first discovers your asset, it
> adds an entry to `dag_overrides.toml` with `enabled = false`. The DAG file is
> created with `schedule = None` (visible in Airflow but won't auto-run). To
> activate, an operator sets `enabled = true` in the TOML. See the
> [Airflow Deployment Guide](airflow-deployment.md#activating-an-asset) for details.

---

## 8. Next Steps

You now know how to build all three types of assets. To go deeper:

- **[Extending Reference](extending-reference.md)** — full attribute documentation for every asset class field, all four token manager auth patterns, the `build_request()`/`parse_response()`/`build_entity_request()` contracts, the `extract()` hook, shared base classes, and advanced features (classify_error, schema contracts, should_stop, rate limit headers).
- **[How-To Guides](how-to-guides.md)** — task-focused guides for common operations: debug a failed run, test with limited data, add endpoints to existing sources (SonarQube, ServiceNow, GitHub, Jira), set up multi-org runs.
- **[Testing Guide](testing.md)** — full test directory structure, all fixtures, mocking patterns, and debugging tips.
- **[Assets Catalog](assets-catalog.md)** — reference for all built-in assets with design decisions explained. Find the closest existing asset to use as a starting template.
