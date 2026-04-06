"""GitHub repos asset — fetches repositories for a GitHub organization.

The organization is taken from GITHUB_ORGS env var (first value if
comma-separated). For multi-org setups where each org has its own
GitHub App credentials, run one Airflow task per org with the
appropriate secrets injected.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubOrgAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register
from data_assets.core.types import PaginationState


@register
class GitHubRepos(GitHubOrgAsset):
    """Fetches repository metadata for a GitHub organization.

    Org comes from the GITHUB_ORGS env var (first value if comma-separated).
    Uses UPSERT so multiple orgs can be loaded by separate Airflow tasks.
    """

    name = "github_repos"
    target_table = "github_repos"
    org_endpoint = "/repos"
    org_request_params = {"type": "all"}

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

    primary_key = ["full_name"]
    indexes = [
        Index(columns=("owner_login",)),
        Index(columns=("language",)),
        Index(columns=("updated_at",)),
    ]

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
                "owner_login": (repo.get("owner") or {}).get("login", ""),
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
