"""GitHub repos asset — fetches repositories across configured organizations.

Multi-org support: iterates through orgs from GITHUB_ORGS env var.
Pagination state (org index + page) is tracked entirely via checkpoints
so retries resume from the correct org and page.

The sequential extractor calls build_request() on every iteration with the
latest checkpoint, so this asset can switch orgs by reading org_idx from it.
"""

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
from data_assets.extract.token_manager import GitHubAppTokenManager


@register
class GitHubRepos(APIAsset):
    """Fetches repository metadata across all configured GitHub organizations.

    Orgs come from the GITHUB_ORGS env var (comma-separated).
    Paginates through each org sequentially, tracking {org_idx, page} in
    checkpoint so retries resume at the correct position.
    """

    name = "github_repos"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_repos"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("full_name", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("owner_login", "TEXT"),
        Column("private", "TEXT"),
        Column("description", "TEXT", nullable=True),
        Column("language", "TEXT", nullable=True),
        Column("default_branch", "TEXT"),
        Column("created_at", "TIMESTAMPTZ"),
        Column("updated_at", "TIMESTAMPTZ"),
        Column("pushed_at", "TIMESTAMPTZ", nullable=True),
        Column("archived", "TEXT"),
        Column("html_url", "TEXT"),
    ]

    primary_key = ["id"]
    date_column = "updated_at"

    def _get_orgs(self) -> list[str]:
        return [o.strip() for o in os.environ.get("GITHUB_ORGS", "").split(",") if o.strip()]

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None
    ) -> RequestSpec:
        orgs = self._get_orgs()
        org_idx = checkpoint.get("org_idx", 0) if checkpoint else 0
        page = checkpoint.get("next_page", 1) if checkpoint else 1

        org = orgs[min(org_idx, len(orgs) - 1)]
        base = os.environ.get("GITHUB_API_URL", self.base_url)

        return RequestSpec(
            method="GET",
            url=f"{base}/orgs/{org}/repos",
            params={"per_page": 100, "page": page, "type": "all"},
            headers={"Accept": "application/vnd.github+json"},
        )

    def parse_response(
        self, response: list[dict[str, Any]]
    ) -> tuple[pd.DataFrame, PaginationState]:
        if not response:
            return (
                pd.DataFrame(columns=[c.name for c in self.columns]),
                PaginationState(has_more=False),
            )

        records = []
        for repo in response:
            records.append({
                "id": repo["id"],
                "full_name": repo["full_name"],
                "name": repo.get("name"),
                "owner_login": repo.get("owner", {}).get("login", ""),
                "private": str(repo.get("private", False)).lower(),
                "description": repo.get("description"),
                "language": repo.get("language"),
                "default_branch": repo.get("default_branch", "main"),
                "created_at": repo.get("created_at"),
                "updated_at": repo.get("updated_at"),
                "pushed_at": repo.get("pushed_at"),
                "archived": str(repo.get("archived", False)).lower(),
                "html_url": repo.get("html_url", ""),
            })
        df = pd.DataFrame(records)

        has_more = len(response) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None)
