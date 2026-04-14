"""Shared helpers and base class for GitHub assets."""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from data_assets.core.api_asset import APIAsset
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import GitHubAppTokenManager


def get_github_org() -> str:
    """Return the first GitHub org from GITHUB_ORGS env var."""
    orgs = [o.strip() for o in os.environ.get("GITHUB_ORGS", "").split(",") if o.strip()]
    if not orgs:
        raise RuntimeError("GITHUB_ORGS env var is not set or empty")
    return orgs[0]


_DEFAULT_GITHUB_API_URL = "https://api.github.com"


def get_github_base_url() -> str:
    """Return the GitHub API base URL (supports Enterprise override)."""
    return os.environ.get("GITHUB_API_URL", _DEFAULT_GITHUB_API_URL)


def filter_to_current_org(keys: list) -> list:
    """Filter entity keys (repo full_names) to the current org only."""
    org = os.environ.get("GITHUB_ORGS", "").split(",")[0].strip()
    if not org:
        return keys
    prefix = f"{org.lower()}/"
    return [k for k in keys if str(k).lower().startswith(prefix)]


class GitHubOrgAsset(APIAsset):
    """Base class for GitHub assets scoped to an organization (not per-repo).

    Handles standard org-level pagination and request building. Subclasses set
    org_endpoint (e.g., "/repos", "/members") and implement parse_response().
    """

    source_name = "github"
    target_schema = "raw"

    token_manager_class = GitHubAppTokenManager
    base_url = _DEFAULT_GITHUB_API_URL

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FULL

    # Subclass sets this to the org-relative API path (e.g., "/repos", "/members")
    org_endpoint: str = ""
    # Extra query params for build_request (e.g., {"type": "all"} for repos)
    org_request_params: dict[str, Any] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if "org_endpoint" in cls.__dict__ and not cls.org_endpoint:
            raise ValueError(
                f"{cls.__name__} sets org_endpoint to empty string. "
                "Set it to the org-relative API path (e.g., '/repos')."
            )

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None
    ) -> RequestSpec:
        org = get_github_org()
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()

        params: dict[str, Any] = {"per_page": 100, "page": page}
        params.update(self.org_request_params)

        return RequestSpec(
            method="GET",
            url=f"{base}/orgs/{org}{self.org_endpoint}",
            params=params,
            headers={"Accept": "application/vnd.github+json"},
        )


class GitHubRepoAsset(APIAsset):
    """Base class for GitHub assets that fan out by repository.

    Provides shared config (token manager, rate limit, pagination, org
    filtering, entity key injection) and helper methods for building
    requests and parsing responses. Subclasses only need to define:
      - name, target_table, columns, primary_key
      - build_entity_request() — the URL and params for this endpoint
      - parse_response() — how to extract records from the API response
      - Optionally: load_strategy, default_run_mode, date_column
    """

    source_name = "github"
    target_schema = "raw"

    token_manager_class = GitHubAppTokenManager
    base_url = _DEFAULT_GITHUB_API_URL

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_repos"
    entity_key_column = "repo_full_name"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    def filter_entity_keys(self, keys: list) -> list:
        return filter_to_current_org(keys)

    def classify_error(self, status_code: int, headers: dict) -> str:
        """GitHub 409 = empty repo (no commits/branches). Skip, don't fail."""
        if status_code == 409:
            return "skip"
        return super().classify_error(status_code, headers)

    def _paginated_entity_request(
        self, _entity_key: str, url_path: str,
        checkpoint: dict | None = None,
        extra_params: dict[str, Any] | None = None,
    ) -> RequestSpec:
        """Build a standard paginated GitHub request for an entity endpoint."""
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()
        params: dict[str, Any] = {"per_page": 100, "page": page}
        if extra_params:
            params.update(extra_params)
        return RequestSpec(
            method="GET",
            url=f"{base}{url_path}",
            params=params,
            headers={"Accept": "application/vnd.github+json"},
        )

    def _parse_array_response(
        self, response: list[dict[str, Any]], record_fn,
    ) -> tuple[pd.DataFrame, PaginationState]:
        """Parse a bare JSON array response (most GitHub endpoints)."""
        if not response:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)
        records = [record_fn(item) for item in response]
        df = pd.DataFrame(records)
        has_more = len(response) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None)

    def _parse_wrapped_response(
        self, response: dict[str, Any], items_key: str, record_fn,
    ) -> tuple[pd.DataFrame, PaginationState]:
        """Parse a wrapped response like {"total_count": N, "items_key": [...]}."""
        items = response.get(items_key, [])
        total = response.get("total_count", 0)
        if not items:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)
        records = [record_fn(item) for item in items]
        df = pd.DataFrame(records)
        has_more = len(items) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None, total_records=total)
