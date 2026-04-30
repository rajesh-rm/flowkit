# Extending Reference: Asset Attributes, Patterns, and Contracts

This is the reference companion to the [Build Your First Asset](tutorial-first-asset.md) tutorial. It contains the full attribute documentation, API contracts, token manager patterns, shared base classes, and advanced features you need when building assets beyond the tutorial examples.

For task-oriented quick guides on adding endpoints to existing sources, see [How-To Guides](how-to-guides.md#adding-endpoints-to-existing-sources).

---

## Design Principles

Every change to this codebase must follow these five rules:

1. **Simple and modular** — code must be understandable to anyone reading it for the first time. Just modular enough for extension, not more.
2. **Pure Python, self-sufficient** — the package handles all ETL logic itself. Only Airflow handles scheduling. No delegating core logic to external frameworks.
3. **Simple patterns over complexity** — prefer 10 lines of clear code over importing a library. No unnecessary abstractions.
4. **Battle-tested libraries only** — dependencies must be 5+ years old, popular in data engineering, and carry a liberal open-source license (MIT, Apache 2.0, BSD). Current deps: SQLAlchemy, pandas, httpx, python-dotenv, PyJWT, pysnc.
5. **90%+ test coverage** — every module has unit tests. Integration tests use mocked APIs + testcontainers (PostgreSQL or MariaDB).

---

## Table of Contents

1. [RestAsset Attributes Reference](#restasset-attributes-reference)
2. [Shared Base Classes](#shared-base-classes)
3. [Sensitive Data and Tokenization](#sensitive-data-and-tokenization)
4. [Token Manager Patterns](#token-manager-patterns)
5. [Asset Class: Every Attribute Explained](#asset-class-every-attribute-explained)
6. [build_request() Contract](#build_request-contract)
7. [parse_response() Contract](#parse_response-contract)
8. [build_entity_request() Contract](#build_entity_request-contract)
9. [The extract() Hook (Custom Client Pattern)](#the-extract-hook-custom-client-pattern)
10. [Key Design Decisions](#key-design-decisions)
11. [Troubleshooting Checklist](#troubleshooting-checklist)
12. [Advanced Features Reference](#advanced-features-reference)

---

## RestAsset Attributes Reference

For the 80% of assets that follow a standard REST API pattern, **RestAsset eliminates the need to write `build_request()` and `parse_response()` entirely.** You declare the endpoint, pagination, and field mapping as class attributes.

**Use RestAsset when:** the API returns JSON, uses standard pagination (page number, offset, or cursor), and you just need to extract fields from the response.

**Use APIAsset instead when:** you need custom request logic (multi-endpoint iteration, computed query parameters like JQL, keyset pagination with composite keys, GraphQL POST + body-embedded cursor).

| Use RestAsset when... | Use APIAsset when... |
|----------------------|---------------------|
| Standard REST: GET endpoint returns JSON with records array | API needs custom request logic (multi-org iteration, JQL construction) |
| Pagination is page_number, offset, or cursor | Pagination needs keyset, custom sort params, or a cursor embedded in a POST body |
| Field mapping is just renames | Response parsing needs nested extraction or type conversion (e.g., GraphQL's `data.<connection>.nodes`) |
| No incremental date filter needed (FULL_REPLACE) | Incremental needs sort-by-update or should_stop() |
| Transport is GET only | Transport is POST (e.g., GraphQL queries with a JSON body) |

**Example:** `sonarqube_projects` uses RestAsset with a custom `extract()` override (handles the 10k ES limit via query sharding). `sonarqube_issues` uses APIAsset (needs UPDATE_DATE sort). `github_deployments` uses APIAsset with `RequestSpec.body` for GraphQL POST (see the [GraphQL transport note](#graphql-transport-note) under `parse_response()` Contract).

| Attribute | Required | Description |
|-----------|----------|-------------|
| `endpoint` | Yes | API path (e.g., `/api/items`) |
| `base_url_env` | Yes | Env var name for base URL (e.g., `"MY_API_URL"`) |
| `response_path` | Yes | Dot-path to records in response JSON. Omit (or set to `None`) if the response itself IS the list — a warning is logged if the response is not a list and no `response_path` is configured. |
| `pagination` | Yes | Dict: `{"strategy": "page_number|offset|cursor|none", "page_size": 100}`. Param name overrides: `page_size_param`, `page_number_param`, `limit_param`, `offset_param`, `page_index_path`. |
| `field_map` | No | Dict mapping API field names → column names. Only for renames. Duplicate column targets are rejected at class definition time (`ValueError`). |
| `api_date_param` | No | Query param name for incremental date filter (e.g., `"updated_since"`) |

---

## Shared Base Classes

When multiple assets share the same API pattern (same auth, pagination, response format), extract a base class. The codebase has five examples:

**ServiceNow** — `ServiceNowTableAsset` uses pysnc (GlideRecord) for extraction via the `extract()` hook. Authentication is handled by `ServiceNowTokenManager` (set as `token_manager_class` on the base). Subclasses only set `name`, `target_table`, `table_name`, and `columns`:

```python
# servicenow/base.py
class ServiceNowTableAsset(APIAsset):
    table_name: str = ""  # subclass sets this
    # ... shared pysnc extraction logic (see extract() hook section) ...

# servicenow/tables.py — all ServiceNow assets in one file
@register
class ServiceNowIncidents(ServiceNowTableAsset):
    name = "servicenow_incidents"
    table_name = "incident"
    columns = [...]
```

**GitHub** — `GitHubRepoAsset` (in `assets/github/helpers.py`) provides shared config for all entity-parallel assets that fan out by repository. It sets `token_manager_class`, `rate_limit_per_second`, `pagination_config`, `parent_asset_name = "github_repos"`, `entity_key_column = "repo_full_name"`, and provides helper methods for building requests and parsing responses. The base class is **transport-agnostic** — both REST assets (branches, commits, PRs, workflows, …) and GraphQL assets (`github_deployments`) inherit from it. GraphQL subclasses override the pagination config to `cursor`, set `entity_key_column = None` plus an `entity_key_map` dict, and build a POST `RequestSpec` with `body={...}` instead of using the `_paginated_entity_request` helper:

```python
# assets/github/helpers.py
class GitHubRepoAsset(APIAsset):
    # Shared: token_manager, rate_limit, pagination, parent_asset, entity_key_column
    def _paginated_entity_request(self, entity_key, url_path, checkpoint, extra_params=None) -> RequestSpec: ...
    def _parse_array_response(self, response, record_fn) -> tuple[DataFrame, PaginationState]: ...
    def _parse_wrapped_response(self, response, items_key, record_fn) -> tuple[DataFrame, PaginationState]: ...

# assets/github/branches.py — 36 lines total
@register
class GitHubBranches(GitHubRepoAsset):
    name = "github_branches"
    target_table = "github_branches"
    columns = [...]
    primary_key = ["repo_full_name", "name"]

    def build_entity_request(self, entity_key, context, checkpoint=None):
        return self._paginated_entity_request(entity_key, f"/repos/{entity_key}/branches", checkpoint)

    def parse_response(self, response):
        return self._parse_array_response(response, lambda b: {
            "repo_full_name": "",  # injected by entity_key_column
            "name": b["name"],
            "protected": str(b.get("protected", False)).lower(),
            "commit_sha": b.get("commit", {}).get("sha", ""),
        })
```

Each repo-scoped GitHub asset is ~35 lines. The base class handles all shared config, entity key injection, org filtering, and request/response boilerplate.

**GitHub (org-scoped)** — `GitHubOrgAsset` (in `assets/github/helpers.py`) provides shared config for org-level sequential assets (repos, members, runner groups). It handles org-scoped request building and pagination. Subclasses set `org_endpoint` (e.g., `"/repos"`, `"/members"`) and optionally `org_request_params`, then implement `parse_response()`:

```python
# assets/github/helpers.py
class GitHubOrgAsset(APIAsset):
    org_endpoint: str = ""          # subclass sets (e.g., "/repos")
    org_request_params: dict = {}   # optional extra query params

    def build_request(self, context, checkpoint=None) -> RequestSpec:
        # builds /orgs/{org}{org_endpoint} with pagination

# assets/github/repos.py
@register
class GitHubRepos(GitHubOrgAsset):
    name = "github_repos"
    org_endpoint = "/repos"
    org_request_params = {"type": "all"}
    # ... only columns, primary_key, indexes, and parse_response() needed
```

**SonarQube** — `SonarQubeAsset` (in `assets/sonarqube/helpers.py`) provides shared config for all 7 entity-parallel SonarQube assets. It sets `token_manager_class`, `source_name`, `target_schema`, `rate_limit_per_second`, an `api_url` property (resolves `SONARQUBE_URL` from env), metric constants (`ALL_METRICS` for the component endpoint, `HISTORY_METRICS` for search_history), and a `parse_paging()` helper for standard pagination:

```python
# assets/sonarqube/helpers.py
class SonarQubeAsset(APIAsset):
    source_name = "sonarqube"
    target_schema = "raw"
    token_manager_class = SonarQubeTokenManager
    rate_limit_per_second = 5.0

    @property
    def api_url(self) -> str:
        return os.environ.get("SONARQUBE_URL", self.base_url)

# assets/sonarqube/branches.py — extends SonarQubeAsset
@register
class SonarQubeBranches(SonarQubeAsset):
    name = "sonarqube_branches"
    target_table = "sonarqube_branches"
    # ... only columns, build_entity_request, and parse_response needed
```

`SonarQubeProjects` uses `RestAsset` instead (for its declarative features) and sets the shared config attributes directly since `RestAsset` and `SonarQubeAsset` are separate base classes.

**Jira** — `JiraAsset` (in `assets/jira/helpers.py`) provides shared config for Jira assets. It sets `source_name`, `token_manager_class = JiraTokenManager`, `rate_limit_per_second`, and provides a `get_jira_url()` helper:

```python
# assets/jira/helpers.py
class JiraAsset(APIAsset):
    source_name = "jira"
    target_schema = "raw"
    token_manager_class = JiraTokenManager
    rate_limit_per_second = 5.0

    def get_jira_url(self) -> str:
        return os.environ.get("JIRA_URL", self.base_url)

# assets/jira/projects.py — extends JiraAsset
@register
class JiraProjects(JiraAsset):
    name = "jira_projects"
    # ... only columns, pagination, build_request, and parse_response needed
```

---

## Sensitive Data and Tokenization

PII columns (user IDs, emails, names, …) can be tokenized via an external HTTP service before any DB write — including the temp table. The framework enforces declarative semantics so misuse fails at registration time, before any data flows.

### The two declarations

```python
from data_assets.core.column import Column
from sqlalchemy import Text

class MyAsset(APIAsset):
    contains_sensitive_data = True   # mandatory on every asset; True or False

    columns = [
        Column("user_id", Text(), nullable=False, sensitive=True),
        Column("display_name", Text()),  # not sensitive
    ]
```

- **`contains_sensitive_data`** (`bool`, mandatory): Set on every concrete asset. The default sentinel `None` is rejected at registration — the choice is never made by accident. Source-base classes (`GitHubOrgAsset`, `JiraAsset`, `ServiceNowTableAsset`, `SonarQubeAsset`, `TransformAsset`, …) declare `False`; subclasses inherit unless they override.
- **`Column(sensitive=True)`** (keyword-only): Marks a column whose values are tokenized before reaching the DB. Default is `False`.

### Validation rules (enforced at registration)

The registry rejects an asset import when any of the following hold:

| Rule | Why |
|---|---|
| `contains_sensitive_data` is `None` | The choice must be explicit. |
| `True` with no `sensitive=True` columns | A `True` flag without any sensitive column is incoherent. |
| `False` with one or more `sensitive=True` columns | The flag and the columns disagree. |
| Sensitive column appears in an explicit `Index.columns` | Indexing tokenized values via an `Index` object is forbidden. |
| Sensitive column appears in an `Index.include` | Same reason. |

**Sensitive columns ARE allowed in `primary_key`.** Some assets are keyed by an inherently sensitive identifier (e.g., a `userID` for which there is no surrogate). The implicit unique index that backs the PK covers tokenized values only — never plaintext — so it does not leak PII. UPSERT on a sensitive PK relies on the tokenization service being deterministic; see "Determinism" below.

### Behavior

- **Per-batch dedup.** Each `write_to_temp` call deduplicates the non-null values in each sensitive column with `dict.fromkeys`, sends the deduplicated list to the endpoint, and remaps every occurrence (including duplicates) before the SQL insert. NULLs pass through unchanged — they are never sent to the API.
- **Extract-only.** Tokenization runs only on the API-extract path. `TransformAsset` reads from already-tokenized `raw.*` tables and never re-tokenizes — preventing double-tokenization that would break joins.
- **Request shape.** Each request POSTs `{"values": [...], "options": {"mode": "opaque", "format": "hex", "token_len": 18}}`. The `options` block tells the service how to shape its tokens; defaults match the standard configuration. Pass `options=...` to the `TokenizationClient` constructor for a different shape (e.g. `token_len=12` for shorter tokens).
- **Response shape.** The service returns `{"tokens": [...], "algo": ..., "namespace": ..., "version": ..., "pii_type_counts": ..., ...}`. The client reads only `tokens` and asserts the array length matches the request; extra metadata fields are tolerated and ignored.
- **Hard fail on API errors.** A `TokenizationError` from the client (after bounded retries on 5xx/timeout/network, or immediate on 4xx) propagates up through `write_to_temp` and aborts the run before any DB write. `@db_retry` does not retry tokenization failures (it only retries DB-transient errors).
- **Determinism.** The tokenization service must return the same token for the same plaintext input across calls. Without this, UPSERT on a sensitive PK produces duplicate rows on every run because the PK never matches an existing row. Confirm this with the service owner before flipping the first asset to `True`. Integration tests pin the assumption with a `f"tok_{v}"` mock.
- **Options stability.** The tokens that determinism guarantees are also keyed on the request `options` (`mode`, `format`, `token_len`). Changing `token_len` from `18` to `12` post-rollout — or any other shape change — yields a different token for the same plaintext, so existing rows in `raw.*` no longer match new ones. Pin `options` at deployment time and treat any change as an explicit re-tokenization step, not a tweak.

### Configuration

Four environment variables (resolved through the same `CredentialResolver` used by source token managers — Airflow Connection → env var → `.env`):

| Variable | Required | Default |
|---|---|---|
| `TOKENIZATION_API_URL` | When any asset has `contains_sensitive_data=True` | — |
| `TOKENIZATION_API_KEY` | No (optional — when unset, the client makes unauthenticated calls) | — |
| `TOKENIZATION_TIMEOUT_SECONDS` | No | `30` |
| `TOKENIZATION_MAX_ATTEMPTS` | No | `3` |

For the operator-facing setup table see [configuration.md](configuration.md#tokenization-service); for the step-by-step recipe see [how-to-guides.md](how-to-guides.md#how-to-enable-tokenization-on-a-sensitive-column).

### Code locations

- `src/data_assets/core/asset.py` — `Asset.contains_sensitive_data`, `Asset.sensitive_column_names()`.
- `src/data_assets/core/column.py` — `Column.sensitive`.
- `src/data_assets/core/registry.py` — `_validate_sensitive_data`.
- `src/data_assets/extract/tokenization_client.py` — HTTP client, `TokenizationError`, `get_default_client()` (thread-safe singleton).
- `src/data_assets/load/tokenization.py` — `apply_tokenization(df, sensitive_columns, client)`.
- `src/data_assets/load/loader.py` — `write_to_temp(..., *, sensitive_columns, tokenization_client)` integration point.

---

## Token Manager Patterns

A token manager is responsible for one thing: giving the HTTP client valid authentication headers. Every API source needs one.

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

**Why the Lock?** When `max_workers > 1`, multiple extraction threads call `get_token()` at the same time. The lock prevents two threads from attempting a token refresh simultaneously, which could cause duplicate HTTP requests or race conditions on the cached token value.

**CredentialResolver** is a helper that finds secrets in this order:
1. Airflow Connection (if running inside Airflow)
2. Environment variable
3. `.env` file (loaded via `python-dotenv`)

You reference it via the module-level `_resolver` instance:

```python
_resolver = CredentialResolver()
```

**Env var naming convention:** `{SOURCE_NAME}_{CREDENTIAL_PART}` in UPPER_SNAKE_CASE. Examples: `PAGERDUTY_TOKEN`, `JIRA_EMAIL`, `GITHUB_PRIVATE_KEY`, `SERVICENOW_CLIENT_ID`.

Below are the four auth patterns you will encounter, from simplest to most complex.

### Pattern 1: Static Token (simplest — like SonarQube)

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
- `get_auth_header` returns the exact header format PagerDuty expects. Different APIs have different conventions (`Bearer`, `Token token=`, etc.).
- No lock needed around `get_token` because the value never changes.

### Pattern 2: Basic Auth (email + token — like Jira Cloud)

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
- Supporting two auth modes (Cloud vs. Data Center) in one class keeps the asset code simple — it just says `token_manager_class = JiraTokenManager`.
- The `base64` import is at function-level because it is only needed for the Cloud path.

### Pattern 3: OAuth2 Client Credentials

> **Note:** ServiceNow assets use pysnc with the `extract()` hook (see
> the extract() hook section below). Authentication is handled by `ServiceNowTokenManager`, which
> supports both OAuth2 and basic auth via its `get_pysnc_auth()` method.
> The pattern below is a reusable template for APIs that use standard
> OAuth2 client_credentials.

The API issues short-lived access tokens. You must acquire one, cache it, and refresh it before it expires.

```python
class ExampleOAuth2TokenManager(TokenManager):
    """OAuth2 client_credentials flow (generic pattern).

    Requires: EXAMPLE_API_URL, EXAMPLE_CLIENT_ID, EXAMPLE_CLIENT_SECRET
    """

    def __init__(self) -> None:
        super().__init__()
        self._instance = _resolver.resolve("EXAMPLE_API_URL") or ""
        self._client_id = _resolver.resolve("EXAMPLE_CLIENT_ID")
        self._client_secret = _resolver.resolve("EXAMPLE_CLIENT_SECRET")
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
            f"{self._instance}/oauth/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=30,  # prevent indefinite hang if token endpoint is down
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._expires_at = time.time() + int(data.get("expires_in", 1800))
```

Key points:
- The lock in `get_token` is critical here. Without it, two threads could both see the token as expired and both fire a refresh request.
- We refresh 60 seconds before actual expiry (`self._expires_at - 60`) to avoid edge cases where the token expires between checking and using it.
- `_refresh` uses a local `httpx` import to avoid circular imports at module load time.
- Always include `timeout=30` on the `httpx.post()` call. Token refresh holds the thread lock — a hanging endpoint blocks all extraction threads.

### Pattern 4: JWT / GitHub App (sign JWT, exchange for installation token)

GitHub Apps authenticate by signing a JWT with a private key, then exchanging it for a short-lived installation token.

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
            timeout=30,  # prevent indefinite hang if token endpoint is down
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["token"]
        self._expires_at = time.time() + 3600  # installation tokens last 1 hour
```

Key points:
- This is a two-step flow: create a JWT (signed locally), then exchange it for a real token via HTTP POST.
- `REFRESH_MARGIN = 300` means we refresh 5 minutes early. GitHub App tokens last 1 hour, so this gives a comfortable buffer.
- The `jwt` and `httpx` libraries are imported inside `_refresh` to keep them as lazy dependencies.
- Always set `timeout=30` on token refresh calls. The refresh holds a thread lock — without a timeout, a hanging token endpoint blocks all extraction threads indefinitely.

### Where to put your new token manager

Add your class to `src/data_assets/extract/token_manager.py` alongside the existing managers. If the file gets too large, you can create a separate module (e.g., `src/data_assets/extract/pagerduty_token_manager.py`) and import it from your asset.

---

## Asset Class: Every Attribute Explained

This section documents every attribute you can set on an `APIAsset` subclass, with annotated examples using a fictional PagerDuty Incidents asset.

### Identity

```python
name = "pagerduty_incidents"
# Globally unique identifier. Used in the registry, CLI, and logs.
# Convention: {source}_{entity} in snake_case.

description = "All incidents from PagerDuty, updated incrementally"
# Human-readable description shown in docs and the asset registry UI.

source_name = "pagerduty"
# Groups related assets. All PagerDuty assets share source_name = "pagerduty".
```

### Target table

```python
target_schema = "raw"
# Convention: "raw" for API-sourced data, "mart" for transforms.

target_table = "pagerduty_incidents"
# The database table name. Convention matches the asset name.
```

### Authentication

```python
token_manager_class = PagerDutyTokenManager
# Points to the token manager class (not an instance).
# The framework instantiates it once per run.
```

### Base URL

```python
base_url = ""
# IMPORTANT: Leave this empty at the class level.
# Read the actual URL from an environment variable inside
# build_request() at runtime. This lets the same code run against
# staging and production by changing an env var.
```

### Rate limiting

```python
rate_limit_per_second = 6.0
# Maximum HTTP requests per second to this API.
# IMPORTANT: This limit is SHARED across all workers.
# If max_workers = 4 and rate_limit_per_second = 6, the total
# request rate is still 6/sec (NOT 24/sec). The rate limiter is
# global, not per-thread.
```

### Pagination

```python
pagination_config = PaginationConfig(
    strategy="offset",
    page_size=100,
)
# strategy options:
#   "page_number" -- API uses ?page=N&per_page=M
#   "offset"      -- API uses ?offset=N&limit=M
#   "cursor"      -- API returns a cursor string for the next page.
#                    Set cursor_field to the JSON key containing
#                    the next-page cursor (e.g., "next_cursor").
#   "none"        -- Single request, no pagination needed.
#
# page_size: how many records to request per page (default: 100).
#
# cursor_field: only used with strategy="cursor". The JSON key in the
#   API response that contains the cursor for the next page.
#
# total_path: only used with strategy="page_number" + PAGE_PARALLEL.
#   Dot-separated path to the total-records field in the response
#   (e.g., "paging.total"). The framework uses this to calculate
#   total pages and fan out parallel fetches.
```

### Parallel extraction

```python
parallel_mode = ParallelMode.NONE
# ParallelMode.NONE (default):
#   Sequential. One page at a time.
#
# ParallelMode.PAGE_PARALLEL:
#   Fetch page 1 first to learn total pages, then fetch 2..N concurrently.
#   Requires: total_path in PaginationConfig.
#
# ParallelMode.ENTITY_PARALLEL:
#   Fan out one request per entity_key from a parent asset.
#   Requires: parent_asset_name, build_entity_request().

max_workers = 1
# Number of concurrent extraction threads.
# All workers share the same rate limiter, so more workers does NOT
# increase the total request rate. It helps when the API has high latency.

entity_key_column = None
# For ENTITY_PARALLEL assets where the API response does NOT include
# the parent entity identifier. The framework injects the entity key as
# this column into every DataFrame after parse_response().
# Leave as None if the response already contains the parent identifier.

entity_key_map = None
# For ENTITY_PARALLEL assets whose parent has a composite PK.
# Maps entity_key dict fields to DataFrame column names.
# Mutually exclusive with entity_key_column.
# Example: sonarqube_measures uses entity_key_map = {"name": "branch"}
```

### Load strategy

```python
load_strategy = LoadStrategy.UPSERT
# LoadStrategy.FULL_REPLACE:
#   Truncate and reload all rows every run. Use for small datasets.
#
# LoadStrategy.UPSERT:
#   INSERT ... ON CONFLICT (primary_key) DO UPDATE. Use for incremental.
#   Requires: primary_key to be set.
#
# LoadStrategy.APPEND:
#   INSERT only, no conflict handling. Use for event logs, audit trails.
```

### Run mode

```python
default_run_mode = RunMode.FORWARD
# RunMode.FULL: Fetch entire dataset from scratch.
# RunMode.FORWARD: Fetch only data since the last successful run.
# RunMode.BACKFILL: Fetch historical data before earliest known coverage.
# RunMode.TRANSFORM: Used by TransformAssets only.
```

### Sensitive data

```python
contains_sensitive_data = False
# Mandatory on every concrete asset (True or False). The default sentinel
# None is rejected at registration time — the choice must be explicit.
# Source-base classes (GitHubOrgAsset, JiraAsset, ServiceNowTableAsset,
# SonarQubeAsset, TransformAsset, …) declare False; subclasses inherit
# unless they override.
#
# When True: at least one Column must have sensitive=True, and no sensitive
# column may appear in any explicit Index.columns or Index.include
# (sensitive columns may stay in primary_key).
#
# See "Sensitive Data and Tokenization" earlier in this document for the
# full validation rules, behavior, and configuration.
```

### Columns

```python
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
    Column("resolved_at", DateTime(timezone=True), nullable=True),
    Column("html_url", Text()),
    Column("raw_json", JSON(), nullable=True),
]
# Column(name, sa_type, nullable=True, default=None)
#
# Common SQLAlchemy types:
#   Text()                   -- strings of any length
#   Integer()                -- whole numbers (32-bit, max ~2.1B)
#   BigInteger()             -- large whole numbers (64-bit) — use for API IDs
#   Float()                  -- floating-point numbers
#   Boolean()                -- true / false
#   DateTime(timezone=True)  -- timestamp with timezone
#   DateTime()               -- timestamp without timezone
#   Date()                   -- date without time
#   JSON()                   -- structured JSON data
#   Numeric()                -- exact decimal numbers
#   Uuid()                   -- UUID values
#
# Import types: from sqlalchemy import Text, Integer, DateTime, JSON, ...
#
# Sensitive (PII) columns:
#   Column("user_id", Text(), nullable=False, sensitive=True)
#   Values are tokenized via the external service before any DB write.
#   Requires the asset to declare contains_sensitive_data = True.
#   Sensitive columns cannot appear in an explicit Index/Index.include
#   (primary_key is allowed). See "Sensitive Data and Tokenization".
#
# MariaDB compatibility notes:
#   - Text() PKs are auto-converted to String(255) / VARCHAR(255)
#   - DateTime(timezone=True) stores as DATETIME on MariaDB (tz-naive)
#   - ISO 8601 datetime strings are auto-converted before writes
```

### Primary key

```python
primary_key = ["id"]
# Used by UPSERT for conflict resolution (ON CONFLICT (id) DO UPDATE).
# Also used by validate() to check for nulls in PK columns.
# For composite keys: primary_key = ["project_key", "incident_id"]
```

### Optional columns (missing-key exemptions)

```python
optional_columns = ["closed_at", "assignee"]
# Columns that MAY be absent from individual API response dicts.
# Listed columns are exempted from the missing-key check (which fails
# the run when a required key is absent from the raw response).
#
# Distinct from Column(nullable=True):
#   - nullable=True   → the DB column may contain NULL values
#   - optional_columns → the API may omit the key entirely from a response
#
# Example: GitHub PRs always include `closed_at` (as null for open PRs),
# so it is NOT optional. SonarQube issues omit `line` entirely for
# file-level issues, so it IS optional.
#
# Guardrail: the registry rejects any entry that is used in primary_key
# or in an index (unique or covering). Identity and lookup columns
# cannot be opted out — schema drift on those is always blocking.
```

### Indexes (required — at least one per asset)

```python
indexes = [
    Index(columns=("status",)),              # filter open/closed
    Index(columns=("updated_at",)),          # time-range queries
    Index(columns=("service_name",)),        # group by service
]
# Index(columns, unique=False, method="btree", where=None, include=None, name=None)
#
#   columns:  Tuple of column names (order matters for composites).
#   unique:   True for UNIQUE indexes.
#   method:   "btree" (default), "gin" (for JSONB), "hash" (for = only).
#   where:    Partial index — raw SQL condition without WHERE keyword.
#   include:  Covering index columns (PostgreSQL only).
#   name:     Auto-generated if omitted as ix_{table}_{cols}[_unique][_partial].
#
# PK columns are already indexed. Index the columns analysts will
# filter, join, or group by.
```

### Column length validation (optional)

```python
column_max_lengths = {
    "status": 100,
    "service_id": 100,
    "html_url": 2048,
}
# When set, validate() blocks promotion if any value exceeds the limit.
# validate_warnings() also warns (non-blocking) for values > 10,000 chars.
#
# Guidelines:
#   - Exact length for fixed-format fields (SHA: 40, GUID: 32)
#   - Generous limits with buffer for variable fields
#   - Omit unbounded user content (descriptions, messages, bios)
```

### Null-rate warning thresholds (optional)

```python
default_null_threshold = 0.02            # Default 2%
column_null_thresholds = {
    "closed_at": 1.0,                    # Silence warning entirely
    "description": 0.5,                  # Warn only if > 50% null
}
# Controls the "High null rate" warning emitted by validate_warnings().
# Non-blocking — promotion proceeds regardless. The warning is a single
# consolidated string listing every offending column, landing in:
#   - Application logs at WARNING level
#   - run_history.metadata["warnings"] as a JSON list
#
# Threshold 1.0 silences the warning for that column (e.g., EAV columns
# that are nullable by design). PK columns are auto-excluded — they
# have a stricter zero-null blocking check in validate().
#
# Null rate and missing-key are different signals:
#   - High null rate: API returns the key with value null (tolerated)
#   - Missing key: API omits the key entirely (blocks via MissingKeyError)
# Use optional_columns for the latter; null thresholds only tune the warning.
```

### Incremental support (date-based watermarks)

```python
date_column = "updated_at"
# The column used to track incremental coverage. The framework stores
# MAX(date_column) after each successful run. On the next FORWARD run,
# context.start_date is set to that stored value.

api_date_param = "since"
# The API query parameter name for date filtering.
# Informational — you must use context.start_date in build_request().
```

### Run resilience

```python
stale_heartbeat_minutes = 20  # default
max_run_hours = 5             # default
# Controls how long a run can be idle or run before being considered
# abandoned. These are defined on the base Asset class and work for
# all asset types.
```

That is a lot of attributes. Here is the mental model:

- **Identity** (name, description, source_name) tells the system what this asset is.
- **Target** (target_schema, target_table, columns, primary_key, indexes) tells the system where data goes and how to index it.
- **Extraction** (token_manager_class, base_url, rate_limit_per_second, pagination_config, parallel_mode, max_workers) tells the system how to get data.
- **Loading** (load_strategy, default_run_mode, date_column, api_date_param) tells the system how to persist data.
- **Run resilience** (stale_heartbeat_minutes, max_run_hours) controls how long a run can be idle or run before being considered abandoned.

---

## build_request() Contract

`build_request()` is called by the extraction framework to find out what HTTP request to make. It is called once per page for sequential and page-parallel extraction, and once for the initial (unscoped) call in entity-parallel mode.

```python
def build_request(
    self,
    context: RunContext,
    checkpoint: dict | None = None,
) -> RequestSpec:
```

**Arguments:**

- `context` — A `RunContext` dataclass (immutable) with:
  - `run_id` (UUID) — unique identifier for this run
  - `mode` (RunMode) — FULL, FORWARD, or BACKFILL
  - `asset_name` (str) — this asset's name
  - `start_date` (datetime | None) — lower bound for incremental extraction. In FORWARD mode this is the high watermark from the last run. In FULL mode this is None.
  - `end_date` (datetime | None) — upper bound (usually now)
  - `params` (dict) — any extra parameters passed at invocation time

- `checkpoint` — A dict with saved pagination state, or `None` on the first call. Common keys:
  - `"page"` or `"next_page"` — for page_number pagination
  - `"next_offset"` — for offset pagination
  - `"cursor"` — for cursor pagination

**Must return:** A `RequestSpec` with:
- `method` — HTTP method string: `"GET"`, `"POST"`, etc.
- `url` — Full URL to the endpoint
- `params` — Query parameters (dict or None)
- `headers` — Extra headers (dict or None). Auth headers are added automatically from your token manager.
- `body` — Request body for POST/PUT (dict or None)

**Example: PagerDuty incidents with offset pagination:**

```python
def build_request(
    self,
    context: RunContext,
    checkpoint: dict | None = None,
) -> RequestSpec:
    base = os.environ.get("PAGERDUTY_URL", "https://api.pagerduty.com")
    offset = checkpoint.get("next_offset", 0) if checkpoint else 0

    params: dict[str, Any] = {
        "limit": self.pagination_config.page_size,
        "offset": offset,
        "sort_by": "created_at:desc",
    }

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
- `os.environ.get("PAGERDUTY_URL", ...)` lets you override the base URL without changing code.
- Always guard against `checkpoint` being `None` (first page) versus an empty dict.
- Auth headers are NOT included here — the framework merges them automatically.

---

## parse_response() Contract

`parse_response()` is called with the raw JSON response from the API. Your job is to extract the records into a DataFrame and tell the framework whether there are more pages.

```python
def parse_response(
    self,
    response: dict,  # or list, depending on the API
) -> tuple[pd.DataFrame, PaginationState]:
```

**Must return:** A tuple of:

1. **DataFrame** — Column names MUST exactly match the `name` fields in your asset's `columns` list.

2. **PaginationState** — Tells the framework whether to fetch another page:
   - `has_more` (bool) — `True` if there are more pages to fetch.
   - `cursor` (str | None) — For cursor-based pagination.
   - `next_offset` (int | None) — For offset-based pagination.
   - `next_page` (int | None) — For page-number pagination.
   - `total_pages` (int | None) — For page-parallel mode.
   - `total_records` (int | None) — Informational total from the API.

**Example: PagerDuty incidents (offset pagination):**

```python
def parse_response(
    self,
    response: dict[str, Any],
) -> tuple[pd.DataFrame, PaginationState]:
    incidents = response.get("incidents", [])

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
            "raw_json":        inc,
        })

    df = pd.DataFrame(records, columns=[c.name for c in self.columns])

    has_more = response.get("more", False)
    offset = response.get("offset", 0)
    limit = response.get("limit", 100)
    next_offset = offset + limit

    return df, PaginationState(
        has_more=has_more,
        next_offset=next_offset,
        total_records=response.get("total"),
    )
```

Key details:
- **Column names must match.** The framework matches by name, not by position.
- **Nested JSON fields** need to be flattened manually.
- **Storing raw JSON** as JSONB (`"raw_json": inc`) is useful for debugging.
- **Always specify `columns=` in the DataFrame constructor** to guarantee consistent column presence even with zero records.

### Missing-key check — required-keys declaration

Every custom `parse_response()` must declare which API fields back each column, so the framework can catch schema drift (a required key going missing) before the DataFrame collapses "absent" and "null" into a single `NaN`. Call `self._check_required_keys(records, field_to_column)` once, with the raw list of response dicts and an explicit mapping from dotted API path → DataFrame column name. It raises `MissingKeyError` on the first record where a non-optional column's path is absent:

```python
def parse_response(self, response):
    incidents = response.get("incidents", [])

    self._check_required_keys(incidents, {
        "id":                     "id",
        "incident_number":        "incident_number",
        "title":                  "title",
        "service.id":             "service_id",       # nested — dotted path
        "service.summary":        "service_name",
        "priority.summary":       "priority_name",
        "created_at":             "created_at",
        "last_status_change_at":  "updated_at",
        "resolved_at":            "resolved_at",
        "html_url":               "html_url",
    })

    # …continue with the flattening loop and DataFrame construction…
```

Rules:
- **Explicit map, no defaults.** Identity maps (`{col: col}`) only work when every column is a top-level response key; for nested payloads (`fields.assignee.displayName` → `assignee`) a default would silently mis-report. `RestAsset` derives the map automatically from `field_map`; custom asset authors must pass one.
- **Skip entity-injected columns.** If the framework fills a column via `entity_key_column` or `entity_key_map` after `parse_response()` returns (e.g., `repo_full_name` injected by `_inject_entity_key`), leave that column out of the map — the API never sent it.
- **Null parents are tolerated.** `{"fields": {"assignee": null}}` with path `fields.assignee.displayName` does **not** trip the check — the API explicitly said "no sub-entity here." Only a genuinely absent key fires.
- **Opt out per column via `optional_columns`.** If the API legitimately omits a key for some records (e.g., SonarQube issues at file level have no `line`), list the column name in the asset's `optional_columns` attribute.

### GraphQL transport note

GraphQL endpoints (like GitHub's `/graphql`) are a POST against a single URL with a JSON body carrying the query and variables. The existing infrastructure already supports this — `RequestSpec.body` is passed to `httpx.request(json=...)` by the API client — so a GraphQL asset is just an `APIAsset` subclass. Two terms used below: `pageInfo` is a GraphQL connection's pagination envelope, and `endCursor` is the opaque "resume here" token it hands back.

1. Set `pagination_config = PaginationConfig(strategy="cursor", page_size=N)`. The framework threads the cursor through `PaginationState.cursor → checkpoint["cursor"] → next build_entity_request()` opaquely; your code reads `checkpoint["cursor"]` and puts it in a body variable (any name — e.g., `"cursor"`) that the GraphQL query text binds to the connection's `after:` argument (`deployments(..., after: $cursor)`).
2. Return a POST `RequestSpec` from `build_request()` / `build_entity_request()`:

   ```python
   return RequestSpec(
       method="POST",
       url=f"{get_github_base_url()}/graphql",
       body={"query": _QUERY, "variables": {"owner": ..., "repo": ..., "cursor": checkpoint.get("cursor") if checkpoint else None}},
       headers={"Accept": "application/vnd.github+json"},
   )
   ```
3. Start `parse_response()` with two guards **before** extracting records — because GraphQL returns HTTP 200 for query/permission errors and non-dict bodies surface on proxy rewrites or maintenance HTML:

   ```python
   if not isinstance(response, dict):
       raise ValueError(f"GraphQL response for {self.name} is not a JSON object: got {type(response).__name__}")
   if errors := response.get("errors"):
       raise ValueError(f"GraphQL error from {self.name}: {errors[0].get('message', errors)}")
   ```
4. Extract the connection and call `self._check_required_keys(nodes, field_to_column)` with an explicit dotted-path map (e.g., `"creator.login": "creator_login"`), then return `PaginationState(has_more=pageInfo["hasNextPage"], cursor=pageInfo["endCursor"])`.

Working reference: `src/data_assets/assets/github/deployments.py`. It inherits `GitHubRepoAsset`, uses dict-shaped entity keys via `entity_key_map` (so `organization` / `repo_name` / `org_repo_key` are injected post-parse from the `{owner, name, full_name}` entity key), and overrides `should_stop()` to halt pagination when the page's oldest `createdAt` falls below the stop threshold described in [assets-catalog.md](assets-catalog.md#key-design-choices).

---

## build_entity_request() Contract

Only needed for `parallel_mode = ParallelMode.ENTITY_PARALLEL`.

**How it works:**
1. The framework looks up the parent asset by `parent_asset_name`.
2. It reads all primary-key values from the parent asset's table.
3. For each value, it calls `build_entity_request(entity_key=...)`.

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
        project_key=entity_key,
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
- `entity_key` is a single value from the parent's primary key column. If the parent has a composite PK, the framework passes a tuple.
- Entity-parallel assets do NOT need to implement `build_request()` — the framework provides a default that delegates to `build_entity_request()`.
- `parse_response()` is shared between entity-parallel and sequential modes.

---

## The extract() Hook (Custom Client Pattern)

For APIs with an official Python client that handles HTTP, auth, and pagination natively, you can bypass the APIClient/httpx pipeline entirely by overriding the `extract()` method.

**When to use:** The data source provides a Python SDK (e.g., pysnc for ServiceNow) that handles authentication and pagination internally.

**The contract** (defined on the base `Asset` class in `core/asset.py`):

```python
def extract(
    self, engine: Engine, temp_table: str, context: RunContext,
) -> int | None:
    """Override to bypass the standard API pipeline.

    Return the number of rows extracted. Return None to fall back
    to the default extraction pipeline.
    """
```

**How it works:** The runner checks whether the asset class overrides `extract()`. If it does, the runner calls it directly instead of going through `build_request()` → `APIClient` → `parse_response()`. Your `extract()` method is responsible for fetching data and writing it to the temp table via `write_to_temp()`.

**Real example: ServiceNow (pysnc)**

All 14 ServiceNow assets use this pattern. `ServiceNowTableAsset` sets `token_manager_class = ServiceNowTokenManager`, and `extract()` uses the token manager's `get_pysnc_auth()` method to get credentials for the pysnc client:

```python
# assets/servicenow/base.py (simplified)
class ServiceNowTableAsset(APIAsset):
    token_manager_class = ServiceNowTokenManager

    def _create_pysnc_client(self):
        from pysnc import ServiceNowClient
        token_mgr = self.token_manager_class()
        return ServiceNowClient(token_mgr.instance, token_mgr.get_pysnc_auth())

    def extract(self, engine, temp_table, context):
        client = self._create_pysnc_client()
        gr = client.GlideRecord(self.table_name, batch_size=1000)
        gr.fields = [c.name for c in self.columns]

        if context.start_date:
            gr.add_query("sys_updated_on", ">=",
                          context.start_date.strftime("%Y-%m-%d %H:%M:%S"))

        gr.query()

        total_rows = 0
        batch = []
        for record in gr:
            batch.append(record.serialize())
            if len(batch) >= 1000:
                df = self._batch_to_df(batch)
                total_rows += write_to_temp(engine, temp_table, df)
                batch = []
        # ... flush remaining batch ...
        return total_rows
```

`ServiceNowTokenManager.get_pysnc_auth()` returns auth suitable for pysnc's `ServiceNowClient` — either a `(username, password)` tuple for basic auth, or a `ServiceNowPasswordGrantFlow` object for OAuth2.

Subclasses set `name`, `target_table`, `table_name`, `columns`, and `indexes` — no `build_request()` or `parse_response()` needed. See `assets/servicenow/tables.py` for concrete examples — all ServiceNow table assets are defined in a single file (~30 lines each).

**Column validation:** `_batch_to_df()` raises a `ValueError` if any declared column is missing from the API response. This catches schema mismatches early. Extra columns from the API that aren't declared are silently dropped.

**Type coercion:** pysnc returns all values as strings. `_batch_to_df()` auto-coerces based on declared column types — `Boolean()` fields map `"true"`/`"false"` to Python booleans, `Float()` fields are parsed via `pd.to_numeric`, and `DateTime()` fields have empty strings replaced with `None` before parsing. No manual conversion needed in subclasses.

**Unique indexes:** You can declare `Index(columns=("email",), unique=True)` on columns that should be unique. The framework handles edge cases automatically: empty strings are converted to NULL before index creation, and if genuine duplicates exist, the index falls back to non-unique with a logged WARNING. This works on both PostgreSQL and MariaDB.

---

## Dialect Extensions

All dialect-specific SQL lives in `src/data_assets/db/dialect.py`. The base `Dialect` class is abstract; `PostgresDialect` and `MariaDBDialect` are the two concrete implementations. Transforms and framework code that need dialect-dependent SQL call methods on `get_dialect(engine)` rather than branching on backend strings.

### TransformAsset.query contract

```python
def query(self, context: RunContext, dialect: Dialect) -> str: ...
```

The runner passes the live engine's `Dialect` instance into `query()`. Transforms that use only ANSI-portable SQL (`DATE()`, `COALESCE`, `GROUP BY`, standard window functions) can ignore `dialect`. Transforms that need week truncation, date arithmetic, or BIGINT casts must use the expression helpers below — they are the only path to dialect-correct SQL without branching.

### SQL-expression helpers

Each helper takes a SQL fragment (identifier or expression) as a string and returns a dialect-correct fragment suitable for f-string interpolation into a larger query.

| Method | Postgres | MariaDB |
|--------|----------|---------|
| `week_start_from_ts(expr)` | `DATE_TRUNC('week', (expr) AT TIME ZONE 'UTC')::date` | `DATE_SUB(DATE(expr), INTERVAL WEEKDAY(expr) DAY)` |
| `date_add_days(expr, days)` | `((expr) + INTERVAL 'N days')::date` | `DATE_ADD(expr, INTERVAL N DAY)` |
| `cast_bigint(expr)` | `CAST(expr AS BIGINT)` | `CAST(expr AS SIGNED)` |

Worked example: `src/data_assets/assets/transforms/sonarqube_adoption_trend.py::query()` — uses all three helpers plus a RECURSIVE CTE to emit a gap-free weekly spine that runs identically on both backends.

### `UTC_SESSION_SQL` class attribute

```python
PostgresDialect.UTC_SESSION_SQL = "SET TIME ZONE 'UTC'"
MariaDBDialect.UTC_SESSION_SQL  = "SET time_zone = '+00:00'"
```

Read by `attach_utc_session_hook(engine)` in `src/data_assets/db/engine.py`, which wires a SQLAlchemy `"connect"` event listener that runs the statement on every new pooled connection. `get_engine()` calls this automatically; test fixtures in `tests/conftest.py::_create_db_engine` do the same so tests match production session defaults.

### Adding a third backend

1. Subclass `Dialect` in `dialect.py`.
2. Implement the abstract methods: `qi`, `set_query_timeout`, `create_table_kw`, `delete_all_rows`, `dedup_temp_table`, `upsert_sql`, `create_index_ddl`, `drop_table_ddl`, `week_start_from_ts`, `date_add_days`, `cast_bigint`.
3. Override the `_sa_dialect` `@cached_property` to return a SQLAlchemy dialect instance (e.g., `postgresql.dialect()`, `mysql.dialect()`). This is read by the default `column_ddl` to compile SQLAlchemy column types to backend-specific DDL — without it, table creation raises `NotImplementedError` at first use.
4. Set the `UTC_SESSION_SQL` class attribute to the backend's session-TZ-to-UTC statement.
5. Wire your dialect into `get_dialect(engine)` by adding a case for your `engine.dialect.name` in the factory.

`fqn`, `column_ddl`, `adjust_pk_columns`, and `prepare_dataframe` are **concrete with defaults** — override only if the backend has unusual constraints (e.g., MariaDB can't use `TEXT` in primary keys, so it overrides `adjust_pk_columns` to convert to `VARCHAR(255)`).

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Load strategies | Full replace, upsert, append | Covers all ETL patterns |
| Failure model | Temp table + checkpoints | Zero wasted API calls on retry |
| Transform safety | Per-query `statement_timeout` (default 300s, configurable per asset) | Prevents runaway SQL from holding connections indefinitely |
| Bulk write safety | `chunksize=1000` on temp table inserts | Prevents bind-parameter overflow on large DataFrames |
| Declarative indexes | Every asset declares `indexes` (at least one required); created after promotion via `CREATE INDEX IF NOT EXISTS` | Proactive query performance — indexes reflect expected query patterns, not reactive DBA work |
| Schema management | Auto-create, additive migration via SchemaContract enum (EVOLVE/FREEZE/DISCARD) | Safe evolution, no data loss |
| Rate limiting | In-process sliding-window counter (thread-safe) | Simple, no external state |
| Parallelism | Thread pool for page/entity fan-out | Shared rate limiter + token manager |
| DB layer | SQLAlchemy ORM (metadata) + Core (DDL) | Best of both worlds |
| In-memory format | pandas DataFrames | Standard, well-supported |
| Multi-org isolation | `partition_key` on locks + watermarks | Same asset, concurrent orgs, no lock collision |

---

## Troubleshooting Checklist

This section covers errors you hit while *building* an asset (registration, request shaping, response parsing). For setup, environment, and runtime errors that show up while *running* an asset, see [how-to-guides.md → How to debug a failed run](how-to-guides.md#how-to-debug-a-failed-run) — it has tabular error-to-fix mappings and a step-by-step triage walkthrough.

### Asset not appearing in the registry?

- **Check the `@register` decorator.** Every asset class must have `@register` directly above the class definition.
- **Check `__init__.py` imports.** The source directory (`assets/pagerduty/__init__.py`) must import the asset class. The auto-discovery mechanism works by importing packages, which triggers `@register`.
- **Check the top-level `assets/__init__.py`.** It must have `import data_assets.assets.pagerduty` (your package name).
- **Check for import errors.** If your module has a syntax error or a missing dependency, the import fails silently (logged at ERROR level). Run your asset module directly to see the traceback:
  ```bash
  python -c "from data_assets.assets.pagerduty.incidents import PagerDutyIncidents"
  ```

### build_request() not being called?

- **Check `parallel_mode`.** If you set `ENTITY_PARALLEL`, the framework calls `build_entity_request()` instead.
- **Check the registry.** Verify your asset is registered:
  ```python
  from data_assets.core.registry import all_assets
  print(all_assets().keys())
  ```

### API returning errors (401, 403, 429, 500)?

- **401/403 — Authentication failure.**
  - Check that the token manager env vars are set.
  - Check that `get_auth_header()` returns the format the API expects.
  - Check that the token has not expired (for OAuth2 / JWT managers).
- **429 — Rate limited.**
  - Lower `rate_limit_per_second` on the asset.
  - The framework has built-in retry with backoff for 429s, but persistent 429s mean your rate limit is set too high.
- **500 — Server error.**
  - Check the `base_url`. It must be read from an env var at runtime inside `build_request()`, NOT hard-coded at the class level. A common mistake is setting `base_url = os.environ.get("PAGERDUTY_URL")` at class definition time, which reads the env var at import time (before it is set).

### Data not appearing in the target table?

- **Check column names.** The DataFrame column names produced by `parse_response()` must exactly match the `name` fields in your `columns` list. A mismatch causes the data to be silently dropped.
- **Check that `parse_response()` returns rows.** Add a temporary print statement: `print(f"Parsed {len(df)} rows")`.
- **Check the load strategy.** If using `FULL_REPLACE`, the table is truncated before loading. If the extraction fails mid-run, you end up with an empty table.

### Run failing with `MissingKeyError`?

```
Asset 'jira_issues': required column 'assignee' (API field
'fields.assignee.displayName') is absent from response record #3.
If this field is legitimately missing for some responses, add
'assignee' to the asset's optional_columns list.
```

- **Confirm it's absent, not null.** A `null` value does not trigger this — only key absence does. Paste the offending record (the error gives its index) into a REPL and run `"assignee" in record["fields"]` to verify.
- **Real schema drift?** The source API removed the field. Update your column list and `field_to_column` map in `parse_response()` to match, or remove the column if the field is permanently gone.
- **Legitimately optional?** Add the column to `optional_columns`. Note that PK columns and any column used in an index cannot be optional — the registry will reject that at import time.
- **Null parent, unexpected fail?** If the offending field is nested (e.g., `user.login`) and the parent is `null`, that is tolerated by design and should NOT raise. If you see this case, something is off — check the raw record's shape.

### Duplicate data in the table?

- **Check `primary_key`.** UPSERT uses the primary key for conflict resolution. If the primary key is wrong or missing, every row is treated as new.
- **Check `load_strategy`.** If you want merge-by-PK behavior, use `LoadStrategy.UPSERT`, not `APPEND`.
- **Check that the API is not returning duplicates.** Some APIs return overlapping pages if data changes between requests. This is normal for UPSERT assets but causes genuine duplicates for APPEND assets.

### Incremental extraction not filtering by date?

- **Check `date_column` and `api_date_param`.** The framework uses `date_column` to compute the high watermark and sets `context.start_date`. But it is YOUR responsibility to actually use `context.start_date` in `build_request()`. The framework does not automatically add date parameters.
- **Check `default_run_mode`.** If it is `RunMode.FULL`, `context.start_date` will be `None`.

### Transform query returning wrong results?

- **Check `source_tables`.** If you list the wrong tables, the dependency ordering may be incorrect.
- **Check the SQL.** Run the query manually against your database.
- **Check column name alignment.** The SQL `SELECT ... AS column_name` aliases must match the `columns` definition names exactly.

### Sensitive-data declaration rejected at registration?

The registry runs four checks against every asset. Each rejection has a precise message — match yours below.

```
ValueError: Asset 'X' must declare contains_sensitive_data (True or False) at the class level.
```
The class attribute is missing or `None`. Add `contains_sensitive_data = False` (or `True` if it has PII columns). Source-base classes like `GitHubOrgAsset` already declare `False`; if your asset inherits from one of those, you only need to override when flipping to `True`.

```
ValueError: Asset 'X' has contains_sensitive_data=True but no columns are marked sensitive=True. Mark at least one column.
```
You declared the asset has sensitive data but no column carries the `sensitive=True` flag. Either flip the flag back to `False`, or mark the column:
```python
columns = [
    Column("user_id", Text(), nullable=False, sensitive=True),
    Column("display_name", Text()),
]
```

```
ValueError: Asset 'X' marks columns ['user_id'] as sensitive but contains_sensitive_data=False. Set contains_sensitive_data=True.
```
The same disagreement, the other direction — a column says it's sensitive but the asset's flag says no. Set `contains_sensitive_data = True`.

```
ValueError: Asset 'X' has index referencing sensitive columns ['user_id']. Explicit indexes on sensitive columns are not allowed; use the primary_key if uniqueness is needed.
```
A `Column` marked `sensitive=True` cannot appear in any explicit `Index.columns` or `Index.include`. Remove the column from the index, drop that index, or restructure the query plan to use a non-sensitive column. Sensitive columns may stay in `primary_key` (the implicit PK index covers tokenized values, never plaintext).

### Sensitive-data run failing at runtime?

The asset registered cleanly but the run aborts with `TokenizationError`:

- **`TOKENIZATION_API_URL is not set...`** — the endpoint URL env var is missing. Set it (or set up the Airflow Connection `tokenization_api`). The error fires on the first batch's call to `write_to_temp`, so you may see a temp table created and a partial run-history row before the failure — that's expected and gets cleaned up.
- **HTTP 401/403 on a request that previously worked** — the service is fronted by auth that is rejecting unauthenticated calls. `TOKENIZATION_API_KEY` is optional, but when the upstream requires it, set the key (env var or Airflow connection) so the client adds the `Authorization: Bearer ...` header.
- **`Tokenization endpoint returned HTTP 4XX: ...`** — non-retriable. Likely a malformed request body or a 401 (rotate the API key).
- **`Tokenization endpoint returned HTTP 5XX after N attempts`** — the service was unavailable after `TOKENIZATION_MAX_ATTEMPTS` retries (default 3). Retrying the run once the service is back is safe; the previous attempt left no DB state because tokenization fires before any insert.
- **`Tokenization response length mismatch: sent X, received Y`** — endpoint contract violation. Capture the request and response (the client logs WARNING on every retry; check your run logs) and report it to the service owner.
- **Same plaintext, different tokens across runs?** UPSERT on a sensitive PK only converges when the tokenization service is deterministic. If you see duplicate rows accumulating after each run on a sensitive PK column, the service is non-deterministic — escalate to the service owner. The integration test `tests/integration/test_tokenization_end_to_end.py::test_upsert_with_sensitive_pk_is_idempotent` pins this assumption.

For the configuration reference (env vars, defaults), see [configuration.md → Tokenization Service](configuration.md#tokenization-service). For the recipe to enable tokenization on a column, see [how-to-guides.md → How to enable tokenization on a sensitive column](how-to-guides.md#how-to-enable-tokenization-on-a-sensitive-column).

---

## Advanced Features Reference

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

**GitHub override:** `GitHubRepoAsset` adds 409→skip (empty repos return 409 Conflict). All repo-scoped GitHub assets inherit this. See `assets/github/helpers.py`.

**Non-JSON responses:** If an API (or an intermediate proxy/CDN) returns a non-JSON response on a successful HTTP status, the framework catches the parse error and raises a `ValueError` with the URL, status code, and first 200 characters of the response body. This makes proxy/CDN misconfiguration failures immediately diagnosable in logs.

### Schema Contracts (`schema_contract`)

Control what happens when your asset definition has columns not yet in the table. Uses the `SchemaContract` enum (from `data_assets.core.enums`):

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

For APIs without date filters (e.g., GitHub PRs), override `should_stop()` to halt pagination when records are older than the watermark:

```python
def should_stop(self, df: pd.DataFrame, context: RunContext) -> bool:
    """Stop when all PRs on the page are older than the watermark."""
    if context.mode.value != "forward" or not context.start_date:
        return False
    updated = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
    return updated.min() < context.start_date
```

Called after each page is written to the temp table. Return `True` to stop paginating. Default: always `False`.

### Rate Limit Header Extraction

The API client automatically checks `X-RateLimit-Remaining` and `X-RateLimit-Limit` headers. If remaining drops below 10% of the limit, it preemptively pauses to avoid 429 errors. No configuration needed.

### Run Metadata

Every run records operational metrics in `run_history.metadata`:
- `api_calls`, `retries`, `skips`, `rate_limit_pauses`
- `extraction_seconds`, `promotion_seconds`
- `warnings` (non-blocking validation warnings)

Query with: `SELECT metadata FROM data_ops.run_history WHERE asset_name = 'my_asset'`

---

## See also

- [Tutorial: Build Your First Asset](tutorial-first-asset.md) — step-by-step walkthrough of building and testing a new asset
- [How-To Guides](how-to-guides.md) — per-source quick references for adding new endpoints
- [Assets Catalog](assets-catalog.md) — all built-in assets with design decisions
- [Testing Guide](testing.md) — test structure, fixtures, patterns
- [Architecture](architecture.md) — ETL lifecycle and component design
