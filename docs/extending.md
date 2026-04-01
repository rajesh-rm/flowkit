# Extending FlowKit: The Complete Developer Guide

This guide walks you through adding new data sources and transforms to FlowKit.
It assumes you are a junior developer who has not seen the codebase before. Every
concept is explained from scratch, with code examples you can copy and adapt.

---

## Design Principles

Every change to this codebase must follow these five rules:

1. **Simple and modular** — code must be understandable by junior developers and AI agents. Just modular enough for extension, not more.
2. **Pure Python, self-sufficient** — the package handles all ETL logic itself. Only Airflow handles scheduling. No delegating core logic to external frameworks.
3. **Simple patterns over complexity** — prefer 10 lines of clear code over importing a library. No unnecessary abstractions.
4. **Battle-tested libraries only** — dependencies must be 5+ years old, popular in data engineering, and carry a liberal open-source license (MIT, Apache 2.0, BSD). Current deps: SQLAlchemy, pandas, httpx, python-dotenv, PyJWT.
5. **90%+ test coverage** — every module has unit tests. Integration tests use mocked APIs + testcontainers Postgres.

---

## Table of Contents

1. [Before You Start -- Decision Flowchart](#1-before-you-start--decision-flowchart)
1.5. [The Easy Path: RestAsset (Declarative)](#15-the-easy-path-restasset-declarative)
2. [Step-by-Step: Adding a New API Source (APIAsset)](#2-step-by-step-adding-a-new-api-source-apiasset)
   - 2a. [Create a Token Manager](#2a-create-a-token-manager)
   - 2b. [Create the Asset Class -- Every Attribute Explained](#2b-create-the-asset-class--every-attribute-explained)
   - 2c. [Implement build_request()](#2c-implement-build_request)
   - 2d. [Implement parse_response()](#2d-implement-parse_response)
   - 2e. [Implement build_entity_request() (ENTITY_PARALLEL only)](#2e-implement-build_entity_request-entity_parallel-only)
   - 2f. [Wire It Up](#2f-wire-it-up)
3. [Step-by-Step: Adding a Transform Asset](#3-step-by-step-adding-a-transform-asset)
4. [Testing Your Asset](#4-testing-your-asset)
5. [Complete Minimal Example](#5-complete-minimal-example)
6. [Troubleshooting Checklist](#6-troubleshooting-checklist)

---

## 1. Before You Start -- Decision Flowchart

Before you write any code, answer four questions about your data source.
Walk through the tree below from top to bottom.

```
QUESTION 1: Where does the data come from?
  |
  +-- External HTTP API (PagerDuty, GitHub, Jira, etc.)
  |     --> Continue to Question 1b.
  |
  +-- Existing Postgres tables (aggregate, join, reshape)
        --> You need a TransformAsset.  Skip to Section 3.


QUESTION 1b: Is this a standard REST API? (JSON response, pagination, field mapping)
  |
  +-- YES, standard pattern (most APIs)
  |     --> Use RestAsset (declarative, ~25 lines, no code to write).
  |         See Section 1.5 below.
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


QUESTION 3: How should the data be loaded into Postgres?
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

With those four answers in hand, you know exactly which classes and attributes
you need. The rest of this guide shows you how to implement them.

---

## 1.5. The Easy Path: RestAsset (Declarative)

For the 80% of assets that follow a standard REST API pattern — fetch JSON, paginate,
map fields to columns — **RestAsset eliminates the need to write `build_request()` and
`parse_response()` entirely.** You just declare the endpoint, pagination, and field
mapping as class attributes.

**Use RestAsset when:** the API returns JSON, uses standard pagination (page number,
offset, or cursor), and you just need to extract fields from the response.

**Use APIAsset instead when:** you need custom request logic (multi-endpoint iteration,
computed query parameters like JQL, keyset pagination with composite keys).

### Real example: SonarQube Projects

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

    # Parallelism + load behavior
    parallel_mode = ParallelMode.PAGE_PARALLEL
    max_workers = 3
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
```

That's it. No `build_request()`. No `parse_response()`. RestAsset generates both
from the class attributes.

### RestAsset attributes reference

| Attribute | Required | Description |
|-----------|----------|-------------|
| `endpoint` | Yes | API path (e.g., `/api/items`) |
| `base_url_env` | Yes | Env var name for base URL (e.g., `"MY_API_URL"`) |
| `response_path` | Yes | Dot-path to records in response JSON. Use `""` if response IS the list. |
| `pagination` | Yes | Dict: `{"strategy": "page_number\|offset\|cursor\|none", "page_size": 100}`. Param name overrides: `page_size_param`, `page_number_param`, `limit_param`, `offset_param`, `page_index_path`. |
| `field_map` | No | Dict mapping API field names → column names. Only for renames. |
| `api_date_param` | No | Query param name for incremental date filter (e.g., `"updated_since"`) |

### Shared base classes for similar APIs

When multiple assets share the same API pattern (same auth, pagination, response format),
extract a base class. Example: ServiceNow incidents and changes both use the Table API
with keyset pagination — `ServiceNowTableAsset` holds the shared logic, subclasses only
set `table_name` and `columns`:

```python
# servicenow/base.py — shared build_request() and parse_response()
class ServiceNowTableAsset(APIAsset):
    table_name: str = ""  # subclass sets this
    # ... shared keyset pagination logic ...

# servicenow/incidents.py — just identity + columns
@register
class ServiceNowIncidents(ServiceNowTableAsset):
    name = "servicenow_incidents"
    table_name = "incident"
    columns = [...]
```

---

## 2. Step-by-Step: Adding a New API Source (APIAsset)

We will build a fictional **PagerDuty Incidents** asset as the running example.
PagerDuty's REST API returns incidents with offset-based pagination and requires
a static API token.

### 2a. Create a Token Manager

A token manager is responsible for one thing: giving the HTTP client valid
authentication headers. Every API source needs one.

The base class lives in `src/data_assets/extract/token_manager.py`:

```python
class TokenManager(ABC):
    def __init__(self) -> None:
        self._lock = threading.Lock()   # <-- for thread safety

    @abstractmethod
    def get_token(self) -> str: ...

    @abstractmethod
    def get_auth_header(self) -> dict[str, str]: ...
```

**Why the Lock?** When `max_workers > 1`, multiple extraction threads call
`get_token()` at the same time. The lock prevents two threads from attempting
a token refresh simultaneously, which could cause duplicate HTTP requests or
race conditions on the cached token value.

**CredentialResolver** is a helper that finds secrets in this order:
1. Airflow Connection (if running inside Airflow)
2. Environment variable
3. `.env` file (loaded via `python-dotenv`)

You reference it via the module-level `_resolver` instance:

```python
_resolver = CredentialResolver()
```

**Env var naming convention:** `{SOURCE_NAME}_{CREDENTIAL_PART}` in
UPPER_SNAKE_CASE. Examples: `PAGERDUTY_TOKEN`, `JIRA_EMAIL`,
`GITHUB_PRIVATE_KEY`, `SERVICENOW_CLIENT_ID`.

Below are the four auth patterns you will encounter, from simplest to most
complex.

#### Pattern 1: Static Token (simplest -- like SonarQube)

The API gives you a long-lived token. You just return it.

```python
class PagerDutyTokenManager(TokenManager):
    """Static API token for PagerDuty.

    Requires: PAGERDUTY_TOKEN
    """

    def __init__(self) -> None:
        super().__init__()
        self._token = _resolver.resolve("PAGERDUTY_TOKEN") or ""
        if not self._token:
            raise RuntimeError("PagerDutyTokenManager requires PAGERDUTY_TOKEN")

    def get_token(self) -> str:
        return self._token

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Token token={self._token}"}
```

Key points:
- `__init__` resolves the secret once, at construction time.
- `get_auth_header` returns the exact header format PagerDuty expects.
  Different APIs have different conventions (`Bearer`, `Token token=`, etc.).
- No lock needed around `get_token` because the value never changes.

#### Pattern 2: Basic Auth (email + token -- like Jira Cloud)

Some APIs authenticate via HTTP Basic Auth (base64-encoded `user:password`).

```python
class JiraTokenManager(TokenManager):
    """Jira Cloud: email + API token as Basic auth.
    Jira Data Center: Personal Access Token as Bearer auth.

    Cloud requires: JIRA_EMAIL + JIRA_API_TOKEN
    Data Center requires: JIRA_PAT
    """

    def __init__(self) -> None:
        super().__init__()
        self._email = _resolver.resolve("JIRA_EMAIL")
        self._api_token = _resolver.resolve("JIRA_API_TOKEN")
        self._pat = _resolver.resolve("JIRA_PAT")
        self._use_pat = bool(self._pat)

        if not self._use_pat and not (self._email and self._api_token):
            raise RuntimeError(
                "JiraTokenManager requires JIRA_PAT (Data Center) or "
                "JIRA_EMAIL + JIRA_API_TOKEN (Cloud)"
            )

    def get_token(self) -> str:
        if self._use_pat:
            return self._pat
        return self._api_token

    def get_auth_header(self) -> dict[str, str]:
        if self._use_pat:
            return {"Authorization": f"Bearer {self._pat}"}
        import base64
        creds = base64.b64encode(
            f"{self._email}:{self._api_token}".encode()
        ).decode()
        return {"Authorization": f"Basic {creds}"}
```

Key points:
- Supporting two auth modes (Cloud vs. Data Center) in one class keeps the
  asset code simple -- it just says `token_manager_class = JiraTokenManager`.
- The `base64` import is at function-level because it is only needed for the
  Cloud path.

#### Pattern 3: OAuth2 Client Credentials (like ServiceNow)

The API issues short-lived access tokens. You must acquire one, cache it,
and refresh it before it expires.

```python
class ServiceNowTokenManager(TokenManager):
    """OAuth2 client_credentials flow.

    Requires: SERVICENOW_INSTANCE, SERVICENOW_CLIENT_ID,
              SERVICENOW_CLIENT_SECRET
    """

    def __init__(self) -> None:
        super().__init__()
        self._instance = _resolver.resolve("SERVICENOW_INSTANCE") or ""
        self._client_id = _resolver.resolve("SERVICENOW_CLIENT_ID")
        self._client_secret = _resolver.resolve("SERVICENOW_CLIENT_SECRET")
        self._token: str | None = None
        self._expires_at: float = 0.0   # Unix timestamp

    def get_token(self) -> str:
        with self._lock:                             # <-- thread-safe
            if self._token and time.time() < self._expires_at - 60:
                return self._token                   # cached, still valid
            self._refresh()                          # expired or first call
            return self._token

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def _refresh(self) -> None:
        import httpx
        resp = httpx.post(
            f"{self._instance}/oauth_token.do",
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._expires_at = time.time() + int(data.get("expires_in", 1800))
```

Key points:
- The lock in `get_token` is critical here. Without it, two threads could
  both see the token as expired and both fire a refresh request.
- We refresh 60 seconds before actual expiry (`self._expires_at - 60`) to
  avoid edge cases where the token expires between checking and using it.
- `_refresh` uses a local `httpx` import to avoid circular imports at
  module load time.

#### Pattern 4: JWT / GitHub App (sign JWT, exchange for installation token)

GitHub Apps authenticate by signing a JWT with a private key, then exchanging
it for a short-lived installation token.

```python
class GitHubAppTokenManager(TokenManager):
    """GitHub App installation tokens (1-hour validity).

    Requires: GITHUB_APP_ID, GITHUB_PRIVATE_KEY, GITHUB_INSTALLATION_ID
    """

    REFRESH_MARGIN = 300  # refresh 5 minutes before expiry

    def __init__(self) -> None:
        super().__init__()
        self._app_id = _resolver.resolve("GITHUB_APP_ID")
        self._private_key = _resolver.resolve("GITHUB_PRIVATE_KEY")
        self._installation_id = _resolver.resolve("GITHUB_INSTALLATION_ID")
        self._token: str | None = None
        self._expires_at: float = 0.0

        if not all([self._app_id, self._private_key, self._installation_id]):
            raise RuntimeError(
                "GitHubAppTokenManager requires GITHUB_APP_ID, "
                "GITHUB_PRIVATE_KEY, and GITHUB_INSTALLATION_ID"
            )

    def get_token(self) -> str:
        with self._lock:
            if self._token and time.time() < (self._expires_at - self.REFRESH_MARGIN):
                return self._token
            self._refresh()
            return self._token

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def _refresh(self) -> None:
        import jwt
        import httpx

        now = int(time.time())
        payload = {
            "iat": now - 60,        # issued 60s ago (clock skew buffer)
            "exp": now + 600,        # JWT valid for 10 minutes
            "iss": self._app_id,
        }
        encoded_jwt = jwt.encode(payload, self._private_key, algorithm="RS256")

        resp = httpx.post(
            f"https://api.github.com/app/installations/"
            f"{self._installation_id}/access_tokens",
            headers={
                "Authorization": f"Bearer {encoded_jwt}",
                "Accept": "application/vnd.github+json",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["token"]
        self._expires_at = time.time() + 3600  # installation tokens last 1 hour
```

Key points:
- This is a two-step flow: create a JWT (signed locally), then exchange it
  for a real token via HTTP POST.
- `REFRESH_MARGIN = 300` means we refresh 5 minutes early. GitHub App tokens
  last 1 hour, so this gives a comfortable buffer.
- The `jwt` and `httpx` libraries are imported inside `_refresh` to keep them
  as lazy dependencies.

#### Where to put your new token manager

Add your class to `src/data_assets/extract/token_manager.py` alongside the
existing managers. If the file gets too large, you can create a separate module
(e.g., `src/data_assets/extract/pagerduty_token_manager.py`) and import it
from your asset.

---

### 2b. Create the Asset Class -- Every Attribute Explained

Create a new file at `src/data_assets/assets/pagerduty/incidents.py`.
Here is the complete class with every attribute annotated:

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
from data_assets.extract.token_manager import PagerDutyTokenManager


@register
class PagerDutyIncidents(APIAsset):
    """PagerDuty incidents -- all incidents across services."""

    # ---------------------------------------------------------------
    # Identity
    # ---------------------------------------------------------------

    name = "pagerduty_incidents"
    # A globally unique identifier for this asset. Used in the registry,
    # in CLI commands (`run --asset pagerduty_incidents`), and in logs.
    # Convention: {source}_{entity} in snake_case.

    description = "All incidents from PagerDuty, updated incrementally"
    # Human-readable description shown in docs and the asset registry UI.

    source_name = "pagerduty"
    # Groups related assets together. All PagerDuty assets share
    # source_name = "pagerduty". Used for filtering and organization.

    # ---------------------------------------------------------------
    # Target table in Postgres
    # ---------------------------------------------------------------

    target_schema = "raw"
    # Which Postgres schema the table lives in.
    # Convention: "raw" for API-sourced data, "mart" for transforms.

    target_table = "pagerduty_incidents"
    # The Postgres table name. Convention matches the asset name.

    # ---------------------------------------------------------------
    # Authentication
    # ---------------------------------------------------------------

    token_manager_class = PagerDutyTokenManager
    # Points to the token manager class (not an instance).
    # The framework instantiates it once per run.

    # ---------------------------------------------------------------
    # Base URL
    # ---------------------------------------------------------------

    base_url = ""
    # IMPORTANT: Leave this empty at the class level.
    # Read the actual URL from an environment variable inside
    # build_request() at runtime. This lets the same code run against
    # staging and production by changing an env var.
    # See build_request() below for the pattern.

    # ---------------------------------------------------------------
    # Rate limiting
    # ---------------------------------------------------------------

    rate_limit_per_second = 6.0
    # Maximum HTTP requests per second to this API.
    # PagerDuty allows ~960 requests/minute = 16/sec; we stay conservative.
    #
    # IMPORTANT: This limit is SHARED across all workers.
    # If max_workers = 4 and rate_limit_per_second = 6, the total
    # request rate is still 6/sec (NOT 24/sec). The rate limiter is
    # global, not per-thread.

    # ---------------------------------------------------------------
    # Pagination
    # ---------------------------------------------------------------

    pagination_config = PaginationConfig(
        strategy="offset",
        page_size=100,
    )
    # How this API paginates its responses.
    #
    # strategy options (also available as PaginationStrategy enum):
    #   "page_number" -- API uses ?page=N&per_page=M
    #                    Example: SonarQube (?p=1&ps=100)
    #                    Good when: API numbers pages starting from 1.
    #
    #   "offset"      -- API uses ?offset=N&limit=M
    #                    Example: PagerDuty, Jira (?startAt=0&maxResults=100)
    #                    Good when: API uses a start-row offset.
    #
    #   "cursor"      -- API returns a cursor string for the next page.
    #                    Example: Slack, some GraphQL APIs.
    #                    Set cursor_field to the JSON key containing
    #                    the next-page cursor (e.g., "next_cursor").
    #
    #   "none"        -- Single request, no pagination needed.
    #
    # For RestAsset's pagination dict shorthand, you can also override
    # query param names: page_size_param, page_number_param,
    # limit_param, offset_param, page_index_path.
    #                    Example: a config endpoint that returns one object.
    #
    # page_size: how many records to request per page (default: 100).
    #
    # cursor_field: only used with strategy="cursor". The JSON key in the
    #   API response that contains the cursor for the next page.
    #
    # total_field: only used with strategy="page_number" + PAGE_PARALLEL.
    #   Dot-separated path to the total-records field in the response
    #   (e.g., "paging.total"). The framework uses this to calculate
    #   total pages and fan out parallel fetches.

    # ---------------------------------------------------------------
    # Parallel extraction
    # ---------------------------------------------------------------

    parallel_mode = ParallelMode.NONE
    # How (or whether) to parallelize extraction.
    #
    # ParallelMode.NONE (default):
    #   Sequential. One page at a time.
    #   Use when: the API is simple, rate limits are tight, or the
    #   dataset is small enough that parallelism is unnecessary.
    #
    # ParallelMode.PAGE_PARALLEL:
    #   Fetch page 1 first to learn the total number of pages, then
    #   fetch pages 2..N concurrently across max_workers threads.
    #   Use when: the first response tells you the total pages/records
    #   (e.g., SonarQube returns paging.total).
    #   Requires: total_field in PaginationConfig (e.g., "paging.total").
    #
    # ParallelMode.ENTITY_PARALLEL:
    #   Fan out one request per entity_key from a parent asset.
    #   Example: fetch issues for each of 50 Jira projects in parallel.
    #   Use when: you are fetching child data scoped to parent entities.
    #   Requires: parent_asset_name, build_entity_request().

    max_workers = 1
    # Number of concurrent extraction threads.
    # Only meaningful when parallel_mode != NONE.
    # Remember: all workers share the same rate limiter, so more
    # workers does NOT increase the total request rate. It just means
    # more requests are in-flight concurrently, which helps when the
    # API has high latency (the next request starts while the previous
    # one is still in transit).

    # ---------------------------------------------------------------
    # Load strategy
    # ---------------------------------------------------------------

    load_strategy = LoadStrategy.UPSERT
    # How extracted data is written to Postgres.
    #
    # LoadStrategy.FULL_REPLACE:
    #   Truncate the table and reload all rows every run.
    #   Use when: the dataset is small and you always fetch everything.
    #   Example: list of projects (hundreds of rows).
    #
    # LoadStrategy.UPSERT:
    #   INSERT ... ON CONFLICT (primary_key) DO UPDATE.
    #   New rows are inserted; existing rows (same PK) are updated.
    #   Use when: the API returns new and changed records incrementally.
    #   Requires: primary_key to be set.
    #   Example: incidents (fetch recent, merge by incident ID).
    #
    # LoadStrategy.APPEND:
    #   INSERT only, no conflict handling.
    #   Use when: data is append-only (event logs, audit trails) and
    #   you never need to update historical rows.

    # ---------------------------------------------------------------
    # Run mode
    # ---------------------------------------------------------------

    default_run_mode = RunMode.FORWARD
    # The default mode when the asset is triggered without an explicit mode.
    #
    # RunMode.FULL:
    #   Fetch the entire dataset from scratch.
    #   Use as default when you always want all data (FULL_REPLACE assets).
    #
    # RunMode.FORWARD:
    #   Fetch only data since the last successful run.
    #   The framework sets context.start_date to the coverage tracker's
    #   high watermark. Use as default for incremental assets (UPSERT).
    #
    # RunMode.BACKFILL:
    #   Fetch historical data before the earliest known coverage.
    #   Typically triggered manually, not as a default.
    #
    # RunMode.TRANSFORM:
    #   Used by TransformAssets. Do not set this on an APIAsset.

    # ---------------------------------------------------------------
    # Columns
    # ---------------------------------------------------------------

    columns = [
        Column("id", "TEXT", nullable=False),
        Column("incident_number", "INTEGER", nullable=False),
        Column("title", "TEXT"),
        Column("status", "TEXT"),
        Column("urgency", "TEXT"),
        Column("priority_name", "TEXT", nullable=True),
        Column("service_id", "TEXT"),
        Column("service_name", "TEXT"),
        Column("created_at", "TIMESTAMPTZ"),
        Column("updated_at", "TIMESTAMPTZ"),
        Column("resolved_at", "TIMESTAMPTZ", nullable=True),
        Column("html_url", "TEXT"),
        Column("raw_json", "JSONB", nullable=True),
    ]
    # Each Column defines one column in the target Postgres table.
    #
    # Column(name, pg_type, nullable=True, default=None)
    #
    #   name:     Column name in Postgres (and in the DataFrame you produce
    #             in parse_response). MUST match exactly.
    #   pg_type:  Postgres data type as a string. Common choices:
    #               TEXT       -- strings of any length
    #               INTEGER    -- whole numbers (-2B to 2B)
    #               FLOAT      -- floating-point numbers
    #               BOOLEAN    -- true / false
    #               TIMESTAMPTZ-- timestamp with timezone (ISO 8601 strings
    #                             are auto-parsed by Postgres)
    #               JSONB      -- structured JSON data (for nested objects
    #                             you do not want to flatten)
    #               DATE       -- date without time
    #   nullable: True (default) allows NULL values. Set False for columns
    #             that must always have data (primary keys, required fields).
    #   default:  Optional SQL expression for a default value.
    #             Example: "now()" for an auto-populated timestamp.

    # ---------------------------------------------------------------
    # Primary key
    # ---------------------------------------------------------------

    primary_key = ["id"]
    # List of column names that form the primary key.
    # Used by UPSERT to detect conflicts (ON CONFLICT (id) DO UPDATE).
    # Also used by the default validate() method to check for nulls
    # in PK columns.
    # For composite keys: primary_key = ["project_key", "incident_id"]

    # ---------------------------------------------------------------
    # Incremental support (date-based watermarks)
    # ---------------------------------------------------------------

    date_column = "updated_at"
    # The column used to track incremental coverage.
    # The framework stores the MAX value of this column after each
    # successful run. On the next FORWARD run, context.start_date is
    # set to that stored value so you only fetch newer records.

    api_date_param = "since"
    # The API query parameter name for "since" / "updated after" filtering.
    # In build_request(), you would use:
    #   params["since"] = context.start_date.isoformat()
    # This attribute is informational for the framework's coverage
    # tracker. The actual parameter construction happens in your
    # build_request() code.

    # ... build_request, parse_response defined below ...
```

That is a lot of attributes. Here is the mental model:

- **Identity** (name, description, source_name) tells the system what this asset is.
- **Target** (target_schema, target_table, columns, primary_key) tells the system where data goes.
- **Extraction** (token_manager_class, base_url, rate_limit_per_second, pagination_config, parallel_mode, max_workers) tells the system how to get data.
- **Loading** (load_strategy, default_run_mode, date_column, api_date_param) tells the system how to persist data.

---

### 2c. Implement build_request()

`build_request()` is called by the extraction framework to find out what HTTP
request to make. It is called once per page for sequential and page-parallel
extraction, and once for the initial (unscoped) call in entity-parallel mode.

**The contract:**

```python
def build_request(
    self,
    context: RunContext,
    checkpoint: dict | None = None,
) -> RequestSpec:
```

**Arguments:**

- `context` -- A `RunContext` dataclass (immutable) with:
  - `run_id` (UUID) -- unique identifier for this run
  - `mode` (RunMode) -- FULL, FORWARD, or BACKFILL
  - `asset_name` (str) -- this asset's name
  - `start_date` (datetime | None) -- lower bound for incremental extraction.
    In FORWARD mode this is the high watermark from the last run.
    In FULL mode this is None.
  - `end_date` (datetime | None) -- upper bound (usually now)
  - `params` (dict) -- any extra parameters passed at invocation time

- `checkpoint` -- A dict with saved pagination state, or `None` on the first
  call. The keys in this dict come from the `PaginationState` you returned in
  your last `parse_response()` call. Common keys:
  - `"page"` or `"next_page"` -- for page_number pagination
  - `"next_offset"` -- for offset pagination
  - `"cursor"` -- for cursor pagination

**Must return:** A `RequestSpec` with:
- `method` -- HTTP method string: `"GET"`, `"POST"`, etc.
- `url` -- Full URL to the endpoint
- `params` -- Query parameters (dict or None)
- `headers` -- Extra headers (dict or None). Auth headers are added
  automatically by the framework from your token manager, but you can add
  additional headers here (e.g., `Accept`, `Content-Type`).
- `body` -- Request body for POST/PUT (dict or None)

**Example: PagerDuty incidents with offset pagination:**

```python
def build_request(
    self,
    context: RunContext,
    checkpoint: dict | None = None,
) -> RequestSpec:
    # Read the base URL from an environment variable at runtime.
    # NEVER hard-code the URL at class level -- this lets the same code
    # work against staging and production.
    base = os.environ.get("PAGERDUTY_URL", "https://api.pagerduty.com")

    # Read pagination state from the checkpoint dict.
    # On the first call, checkpoint is None, so we default to offset=0.
    offset = checkpoint.get("next_offset", 0) if checkpoint else 0

    # Build query parameters.
    params: dict[str, Any] = {
        "limit": self.pagination_config.page_size,   # 100
        "offset": offset,
        "sort_by": "created_at:desc",
    }

    # For incremental (FORWARD) runs, add a date filter.
    if context.start_date:
        params["since"] = context.start_date.isoformat()

    return RequestSpec(
        method="GET",
        url=f"{base}/incidents",
        params=params,
        headers={"Accept": "application/json"},
    )
```

Key details:
- The `os.environ.get("PAGERDUTY_URL", ...)` pattern means you can override the
  base URL without changing code. The default is the production URL.
- `checkpoint.get("next_offset", 0) if checkpoint else 0` -- always guard
  against `checkpoint` being `None` (first page) versus an empty dict.
- Auth headers are NOT included here. The framework calls
  `token_manager.get_auth_header()` and merges the result automatically.

---

### 2d. Implement parse_response()

`parse_response()` is called with the raw JSON response from the API. Your job
is to extract the records into a DataFrame and tell the framework whether there
are more pages.

**The contract:**

```python
def parse_response(
    self,
    response: dict,  # or list, depending on the API
) -> tuple[pd.DataFrame, PaginationState]:
```

**Arguments:**

- `response` -- The deserialized JSON response (a dict or list). The framework
  calls `response.json()` on the HTTP response and passes the result here.

**Must return:** A tuple of:

1. **DataFrame** -- A `pd.DataFrame` whose column names MUST exactly match the
   `name` fields in your asset's `columns` list. If the API uses different
   field names (e.g., `incident_number` in the API vs. `incident_number` in
   your column), you rename them here.

2. **PaginationState** -- Tells the framework whether to fetch another page:
   - `has_more` (bool) -- `True` if there are more pages to fetch.
   - `cursor` (str | None) -- For cursor-based pagination, the cursor for the next page.
   - `next_offset` (int | None) -- For offset-based pagination, the offset for the next page.
   - `next_page` (int | None) -- For page-number pagination, the next page number.
   - `total_pages` (int | None) -- For page-parallel mode, how many pages exist in total.
   - `total_records` (int | None) -- Informational total from the API.

**Example: PagerDuty incidents (offset pagination):**

```python
def parse_response(
    self,
    response: dict[str, Any],
) -> tuple[pd.DataFrame, PaginationState]:
    # 1. Extract the list of records from the response.
    #    PagerDuty nests them under "incidents".
    incidents = response.get("incidents", [])

    # 2. Flatten each record into a dict matching our column names.
    #    The API field names may differ from our Postgres column names.
    records: list[dict[str, Any]] = []
    for inc in incidents:
        service = inc.get("service") or {}
        priority = inc.get("priority") or {}

        records.append({
            "id":              inc.get("id"),
            "incident_number": inc.get("incident_number"),
            "title":           inc.get("title"),
            "status":          inc.get("status"),
            "urgency":         inc.get("urgency"),
            "priority_name":   priority.get("summary"),
            "service_id":      service.get("id"),
            "service_name":    service.get("summary"),
            "created_at":      inc.get("created_at"),
            "updated_at":      inc.get("last_status_change_at"),
            "resolved_at":     inc.get("resolved_at"),
            "html_url":        inc.get("html_url"),
            "raw_json":        inc,  # store full JSON as JSONB
        })

    # 3. Build the DataFrame. Specify columns to guarantee column order
    #    and ensure all columns exist even if the API returns nothing.
    df = pd.DataFrame(records, columns=[c.name for c in self.columns])

    # 4. Determine if there are more pages.
    #    PagerDuty returns "more": true/false in the response.
    has_more = response.get("more", False)
    offset = response.get("offset", 0)
    limit = response.get("limit", 100)
    next_offset = offset + limit

    # 5. Return the DataFrame and pagination state.
    return df, PaginationState(
        has_more=has_more,
        next_offset=next_offset,
        total_records=response.get("total"),
    )
```

Key details:
- **Column names must match.** If you define `Column("service_name", ...)` in
  your columns list, the DataFrame must have a column called `"service_name"`.
  The framework matches by name, not by position.
- **Nested JSON fields** need to be flattened manually. The API might return
  `{"service": {"id": "ABC", "summary": "My Service"}}`, but your columns
  are `service_id` and `service_name`. Do the extraction here.
- **Storing raw JSON** as JSONB (`"raw_json": inc`) is useful for debugging
  and for fields you might want to extract later without re-fetching.
- **Always specify `columns=` in the DataFrame constructor** to guarantee
  consistent column presence. If the API returns zero records, you still get
  a DataFrame with the right columns (just zero rows).

---

### 2e. Implement build_entity_request() (ENTITY_PARALLEL only)

You only need this method if your asset uses `parallel_mode = ParallelMode.ENTITY_PARALLEL`.

**When to use ENTITY_PARALLEL:** Your asset fetches child data scoped to a
parent entity. For example, Jira issues per project, GitHub commits per
repository, or PagerDuty incidents per service.

**How it works:**
1. The framework looks up the parent asset by `parent_asset_name`.
2. It reads all primary-key values from the parent asset's table.
3. For each value, it calls `build_entity_request(entity_key=...)` to get a
   `RequestSpec`, then fetches and parses pages in parallel across
   `max_workers` threads.

**The contract:**

```python
def build_entity_request(
    self,
    entity_key: Any,        # One value from the parent's primary key column
    context: RunContext,
    checkpoint: dict | None = None,
) -> RequestSpec:
```

**Example: Jira issues per project (from the real codebase):**

```python
# On the asset class:
parallel_mode = ParallelMode.ENTITY_PARALLEL
max_workers = 3
parent_asset_name = "jira_projects"
# The framework reads all "key" values from the jira_projects table
# (because "key" is jira_projects' primary_key column).

def build_entity_request(
    self,
    entity_key: str,              # e.g., "PROJ-A", "PROJ-B"
    context: RunContext,
    checkpoint: dict | None = None,
) -> RequestSpec:
    start_date_iso = None
    if context.start_date:
        start_date_iso = context.start_date.isoformat()

    jql = self._build_jql(
        project_key=entity_key,    # <-- scope to this project
        start_date=start_date_iso,
    )

    start_at = checkpoint.get("next_offset", 0) if checkpoint else 0

    base = os.environ.get("JIRA_URL", self.base_url)
    return RequestSpec(
        method="GET",
        url=f"{base}/rest/api/3/search",
        params={
            "jql": jql,
            "maxResults": 100,
            "startAt": start_at,
            "fields": "summary,status,priority,issuetype,assignee,"
                      "reporter,created,updated,resolutiondate,labels",
        },
    )
```

Key details:
- `entity_key` is a single value (string, int, etc.) from the parent asset's
  primary key column. If the parent has a composite PK, the framework passes
  a tuple.
- You still need `build_request()` as well -- it serves as the fallback for
  non-entity-parallel runs (e.g., when someone runs the asset in FULL mode
  without a parent).
- `parse_response()` is shared between entity-parallel and sequential modes.
  It does not know or care which entity it is parsing.

---

### 2f. Wire It Up

You have a token manager and an asset class. Here is how to make them
discoverable by the framework.

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

This import is essential. The framework discovers assets by walking all
subpackages under `data_assets.assets` and importing them. Importing a module
that contains a `@register`-decorated class triggers the registration.

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

Add your `PagerDutyTokenManager` class to
`src/data_assets/extract/token_manager.py` (see Section 2a).

**How the `@register` decorator works:**

```python
# From src/data_assets/core/registry.py:
_registry: dict[str, type[Asset]] = {}

def register(asset_cls: type[Asset]) -> type[Asset]:
    name = asset_cls.name
    _registry[name] = asset_cls
    return asset_cls
```

It is a simple class decorator. When Python imports a module and encounters
`@register` on a class definition, it calls `register(PagerDutyIncidents)`,
which adds it to the global `_registry` dict keyed by `asset_cls.name`. Later,
the framework calls `registry.get("pagerduty_incidents")` to retrieve it.

**Checklist before you test:**

- [ ] Token manager class exists in `extract/token_manager.py`
- [ ] Asset file exists at `assets/pagerduty/incidents.py`
- [ ] Asset class has `@register` decorator
- [ ] Asset class `name` is unique across all assets
- [ ] `assets/pagerduty/__init__.py` imports the asset class
- [ ] `assets/__init__.py` imports `data_assets.assets.pagerduty`
- [ ] Required env vars are documented / set: `PAGERDUTY_TOKEN`, `PAGERDUTY_URL`

---

## 3. Step-by-Step: Adding a Transform Asset

Transform assets produce derived data from existing Postgres tables. They do
not call any external API. Instead, they run a SQL query against the database
and write the results to a new table.

**When to use:** You already have raw data in Postgres (from API assets) and
you want to create aggregated, joined, or reshaped views of that data.

**The base class:**

```python
class TransformAsset(Asset):
    asset_type = AssetType.TRANSFORM
    default_run_mode = RunMode.TRANSFORM
    load_strategy = LoadStrategy.FULL_REPLACE
    target_schema = "mart"       # convention: transforms go in "mart" schema
    source_schema = "raw"
    source_tables: list[str] = []

    @abstractmethod
    def query(self, context: RunContext) -> str:
        """Return a SQL SELECT producing the output rows."""
        ...
```

**Complete example: daily PagerDuty incident summary.**

Create `src/data_assets/assets/transforms/pagerduty_incident_summary.py`:

```python
from __future__ import annotations

from data_assets.core.column import Column
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

    source_schema = "raw"
    source_tables = ["pagerduty_incidents"]
    # source_tables is informational -- it documents which raw tables
    # this transform depends on. The framework uses it for dependency
    # ordering (run raw assets before their transforms).

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

After the SQL query runs, the framework calls `transform(df)` on the result
DataFrame. The default implementation returns `df` unchanged. Override it if
you need pandas-level post-processing that is hard to express in SQL:

```python
def transform(self, df: pd.DataFrame) -> pd.DataFrame:
    # Example: add a computed column
    df["is_slow_resolution"] = df["avg_resolve_hours"] > 24.0
    return df
```

**Wiring up a transform:**

Transforms go in `src/data_assets/assets/transforms/`. The existing
`__init__.py` imports transform classes. Add your new import:

```python
# src/data_assets/assets/transforms/__init__.py
from data_assets.assets.transforms.incident_summary import IncidentSummary
from data_assets.assets.transforms.pagerduty_incident_summary import PagerDutyIncidentSummary
```

No other wiring is needed -- the `transforms` package is already imported by
the top-level `assets/__init__.py`.

---

## 4. Testing Your Asset

### Test Fixtures

Create a JSON file that matches the shape of a real API response. Save it in
your test directory (e.g., `tests/fixtures/pagerduty/incidents_page1.json`):

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

Instantiate the asset, call `build_request()` and `parse_response()` directly
with test data. No HTTP calls, no database.

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

Use `respx` to mock the HTTP layer and a test Postgres database to verify
end-to-end behavior:

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

---

## 5. Complete Minimal Example

Below is a fully copy-paste-ready "hello world" asset. It fetches from a
fictional `/api/items` endpoint, uses sequential offset pagination, and stores
three columns.

### 5a. Token Manager

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

### 5b. Asset Class

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

### 5c. Register the Package

Add this line to `src/data_assets/assets/__init__.py`:

```python
import data_assets.assets.items_api  # noqa: F401
```

### 5d. Test File

Create `tests/test_items_api.py`:

```python
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

### 5e. Set Environment Variables

```bash
export ITEMS_API_TOKEN="your-api-token-here"
export ITEMS_API_URL="https://items.example.com"
```

---

## 6. Troubleshooting Checklist

### Asset not appearing in the registry?

- **Check the `@register` decorator.** Every asset class must have `@register`
  directly above the class definition. Without it, the class exists but the
  framework does not know about it.
- **Check `__init__.py` imports.** The source directory
  (`assets/pagerduty/__init__.py`) must import the asset class. The auto-discovery
  mechanism works by importing packages, which triggers `@register`.
- **Check the top-level `assets/__init__.py`.** It must have
  `import data_assets.assets.pagerduty` (your package name).
- **Check for import errors.** If your module has a syntax error or a missing
  dependency, the import fails silently (logged at ERROR level). Run your
  asset module directly to see the traceback:
  ```bash
  python -c "from data_assets.assets.pagerduty.incidents import PagerDutyIncidents"
  ```

### build_request() not being called?

- **Check `parallel_mode`.** If you set `ENTITY_PARALLEL`, the framework calls
  `build_entity_request()` instead of `build_request()` for each entity.
  `build_request()` is still called as a fallback for non-parallel runs.
- **Check the registry.** Verify your asset is registered:
  ```python
  from data_assets.core.registry import all_assets
  print(all_assets().keys())
  ```

### API returning errors (401, 403, 429, 500)?

- **401/403 -- Authentication failure.**
  - Check that the token manager env vars are set (`PAGERDUTY_TOKEN`, etc.).
  - Check that `get_auth_header()` returns the format the API expects.
    Some APIs want `Bearer`, some want `Token token=`, some want `Basic`.
  - Check that the token has not expired (for OAuth2 / JWT managers).
- **429 -- Rate limited.**
  - Lower `rate_limit_per_second` on the asset.
  - The framework has built-in retry with backoff for 429s, but persistent
    429s mean your rate limit is set too high.
- **500 -- Server error.**
  - Check the `base_url`. It must be read from an env var at runtime inside
    `build_request()`, NOT hard-coded at the class level. A common mistake
    is setting `base_url = os.environ.get("PAGERDUTY_URL")` at class
    definition time, which reads the env var at import time (before it is
    set).

### Data not appearing in the target table?

- **Check column names.** The DataFrame column names produced by
  `parse_response()` must exactly match the `name` fields in your `columns`
  list. A mismatch (e.g., `service_summary` vs. `service_name`) causes the
  data to be silently dropped.
- **Check that `parse_response()` returns rows.** Add a temporary print
  statement: `print(f"Parsed {len(df)} rows")`.
- **Check the load strategy.** If using `FULL_REPLACE`, the table is truncated
  before loading. If the extraction fails mid-run, you end up with an empty
  table.

### Duplicate data in the table?

- **Check `primary_key`.** UPSERT uses the primary key for conflict resolution.
  If the primary key is wrong or missing, every row is treated as new.
- **Check `load_strategy`.** If you want merge-by-PK behavior, use
  `LoadStrategy.UPSERT`, not `APPEND` (which never deduplicates).
- **Check that the API is not returning duplicates.** Some APIs return
  overlapping pages if data changes between requests. This is normal for
  UPSERT assets (the second copy overwrites the first), but causes genuine
  duplicates for APPEND assets.

### Incremental extraction not filtering by date?

- **Check `date_column` and `api_date_param`.** The framework uses
  `date_column` to compute the high watermark and sets `context.start_date`.
  But it is YOUR responsibility to actually use `context.start_date` in
  `build_request()` to filter the API call. The framework does not
  automatically add date parameters to the request.
- **Check `default_run_mode`.** If it is `RunMode.FULL`, `context.start_date`
  will be `None` and no date filtering happens. Use `RunMode.FORWARD` for
  incremental extraction.

### Transform query returning wrong results?

- **Check `source_tables`.** This is informational, but if you list the wrong
  tables, the dependency ordering may be incorrect (the transform runs before
  its source data is refreshed).
- **Check the SQL.** Run the query manually against Postgres to verify it
  returns what you expect.
- **Check column name alignment.** The SQL `SELECT ... AS column_name` aliases
  must match the `columns` definition names exactly.

---

## 7. Advanced Features Reference

### Error Classification (`classify_error`)

Override on your asset to control how HTTP errors are handled:

```python
def classify_error(self, status_code: int, headers: dict) -> str:
    if status_code == 404:
        return "skip"   # Entity deleted — skip, don't fail
    if status_code == 429 or status_code >= 500:
        return "retry"  # Transient — retry with backoff
    return "fail"       # Client error — fail immediately
```

Default: 404→skip, 429/5xx→retry, other 4xx→fail.

### Schema Contracts (`schema_contract`)

Control what happens when your asset definition has columns not yet in the table.
Uses the `SchemaContract` enum (from `data_assets.core.enums`):

```python
from data_assets.core.enums import SchemaContract

schema_contract = SchemaContract.EVOLVE   # Default: auto ALTER TABLE ADD COLUMN
schema_contract = SchemaContract.FREEZE   # Raise error — no automatic schema changes
schema_contract = SchemaContract.DISCARD  # Silently ignore new columns
```

### Dry Run Mode

Test your asset without writing to the main table:

```python
run_asset("my_asset", run_mode="full", dry_run=True)
# Extracts to temp table, validates, but skips promotion
# Returns status="dry_run" with row counts
```

### Early Stop (`should_stop`)

For APIs without date filters (e.g., GitHub PRs), override `should_stop()` to halt
pagination when records are older than the watermark:

```python
def should_stop(self, df: pd.DataFrame, context: RunContext) -> bool:
    """Stop when all PRs on the page are older than the watermark."""
    if context.mode.value != "forward" or not context.start_date:
        return False
    updated = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
    return updated.min() < context.start_date
```

Called after each page is written to the temp table. Return `True` to stop
paginating. Default: always `False` (let pagination exhaust naturally).

### Passing Secrets from Airflow

`run_asset()` accepts a `secrets` dict — env var names to values. Secrets are
injected into `os.environ` for the duration of the run and cleaned up after:

```python
from airflow.hooks.base import BaseHook
from data_assets import run_asset

def _run_github(**context):
    conn = BaseHook.get_connection("github_app")
    run_asset(
        "github_repos",
        run_mode="full",
        secrets={
            "GITHUB_APP_ID": conn.login,
            "GITHUB_PRIVATE_KEY": conn.password,
            "GITHUB_INSTALLATION_ID": conn.extra_dejson["installation_id"],
            "GITHUB_ORGS": conn.extra_dejson["orgs"],
        },
        airflow_run_id=context["run_id"],
    )
```

Secrets are resolved at execution time on the worker, not at DAG parse time.
With a secret backend (Vault, AWS SSM, GCP Secret Manager), values never touch
Airflow's metadata DB.

### Rate Limit Header Extraction

The API client automatically checks `X-RateLimit-Remaining` and
`X-RateLimit-Limit` headers. If remaining drops below 10% of the limit,
it preemptively pauses to avoid 429 errors. No configuration needed.

### Run Metadata

Every run records operational metrics in `run_history.metadata`:
- `api_calls`, `retries`, `skips`, `rate_limit_pauses`
- `extraction_seconds`, `promotion_seconds`
- `warnings` (non-blocking validation warnings)

Query with: `SELECT metadata FROM data_ops.run_history WHERE asset_name = 'my_asset'`

---

## 8. Quick Reference: Adding Endpoints by Source

### When to use RestAsset vs APIAsset

| Use RestAsset when... | Use APIAsset when... |
|----------------------|---------------------|
| Standard REST: GET endpoint returns JSON with records array | API needs custom request logic (multi-org iteration, JQL construction) |
| Pagination is page_number, offset, or cursor | Pagination needs keyset or custom sort params |
| Field mapping is just renames | Response parsing needs nested extraction or type conversion |
| No incremental date filter needed (FULL_REPLACE) | Incremental needs sort-by-update or should_stop() |

**Example:** `sonarqube_projects` uses RestAsset (simple list). `sonarqube_issues` uses APIAsset (needs UPDATE_DATE sort).

### Adding a SonarQube endpoint

Copy `sonarqube/projects.py` (RestAsset pattern) or `sonarqube/measures.py` (APIAsset with entity-parallel). Key settings:
- `token_manager_class = SonarQubeTokenManager`
- `base_url_env = "SONARQUBE_URL"`
- Pagination: `{"strategy": "page_number", "page_size": 100, "total_path": "paging.total", "page_index_path": "paging.pageIndex"}`
- Response path: check API docs for the key containing the records array
- Add `qualifiers=TRK` for project-scoped endpoints
- SonarQube API docs: https://next.sonarqube.com/sonarqube/web_api
- Reference endpoints: `/api/components/search`, `/api/issues/search`, `/api/measures/component`, `/api/project_branches/list`, `/api/project_analyses/search`

### Adding a ServiceNow endpoint

Copy `servicenow/incidents.py` (APIAsset with keyset pagination). Key settings:
- `token_manager_class = ServiceNowTokenManager`
- Change URL in `build_request()`: `/api/now/table/{table_name}`
- Keyset pagination on `sys_updated_on,sys_id` — copy the exact query syntax
- ServiceNow query syntax: `^` = AND, `^OR` = OR
- Table API docs: https://docs.servicenow.com/bundle/latest/page/integrate/inbound-rest/concept/c_TableAPI.html

### Adding a GitHub endpoint

Copy `github/pull_requests.py` (APIAsset with entity-parallel). Key settings:
- `token_manager_class = GitHubAppTokenManager`
- Pagination: `{"strategy": "page_number", "page_size": 100}`
- GitHub uses `per_page` + `page` params (not `ps`/`p`)
- Always include `Accept: application/vnd.github+json` header
- For child data (commits, reviews): use ENTITY_PARALLEL with `parent_asset_name = "github_repos"`
- **`since` param**: works on `/repos/{o}/{r}/issues` and `/repos/{o}/{r}/commits` but NOT on `/pulls`
- GitHub REST API docs: https://docs.github.com/en/rest

### Adding a Jira endpoint

Copy `jira/issues.py` (APIAsset with JQL + entity-parallel). Key settings:
- `token_manager_class = JiraTokenManager`
- Pagination: `{"strategy": "offset", "page_size": 100}` with `startAt`/`maxResults`
- Use JQL for date filtering: `updated >= "{iso_date}"`
- For entity-parallel: set `parent_asset_name = "jira_projects"` (fans out by project key)
- Jira REST API v3 docs: https://developer.atlassian.com/cloud/jira/platform/rest/v3/
