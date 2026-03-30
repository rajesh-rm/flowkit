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
    """Fetches repository metadata across all configured GitHub organizations."""

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

    def __init__(self) -> None:
        super().__init__()
        self._orgs: list[str] = [
            o.strip()
            for o in os.environ.get("GITHUB_ORGS", "").split(",")
            if o.strip()
        ]
        self._current_org_idx: int = 0

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None
    ) -> RequestSpec:
        if checkpoint:
            self._current_org_idx = checkpoint.get("org_idx", 0)
            page = checkpoint.get("page", 1)
        else:
            self._current_org_idx = 0
            page = 1

        org = self._orgs[self._current_org_idx]
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
            # No repos returned for this org; try next org
            self._current_org_idx += 1
            if self._current_org_idx < len(self._orgs):
                return (
                    pd.DataFrame(columns=[c.name for c in self.columns]),
                    PaginationState(has_more=True, next_page=1),
                )
            return (
                pd.DataFrame(columns=[c.name for c in self.columns]),
                PaginationState(has_more=False),
            )

        records = []
        for repo in response:
            records.append(
                {
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
                }
            )
        df = pd.DataFrame(records)

        has_more_pages = len(response) >= self.pagination_config.page_size

        if not has_more_pages:
            # Current org exhausted, check for more orgs
            self._current_org_idx += 1
            if self._current_org_idx < len(self._orgs):
                return df, PaginationState(has_more=True, next_page=1)
            return df, PaginationState(has_more=False)

        return df, PaginationState(has_more=True, next_page=None)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure boolean-origin columns are stored as text strings."""
        for col in ("private", "archived"):
            if col in df.columns:
                df[col] = df[col].astype(str).str.lower()
        if "owner_login" in df.columns:
            df["owner_login"] = df["owner_login"].astype(str)
        return df
