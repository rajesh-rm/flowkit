"""GitHub Actions runner groups — org-level self-hosted runner group config."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubOrgAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register
from data_assets.core.types import PaginationState


@register
class GitHubRunnerGroups(GitHubOrgAsset):
    """Self-hosted runner groups for the organization."""

    name = "github_runner_groups"
    target_table = "github_runner_groups"
    org_endpoint = "/actions/runner-groups"

    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("name", "TEXT"),
        Column("visibility", "TEXT"),
        Column("default", "TEXT"),
        Column("allows_public_repositories", "TEXT"),
    ]

    primary_key = ["id"]
    indexes = [
        Index(columns=("name",)),
    ]

    def parse_response(
        self, response: dict[str, Any]
    ) -> tuple[pd.DataFrame, PaginationState]:
        groups = response.get("runner_groups", [])
        total = response.get("total_count", 0)

        if not groups:
            return (
                pd.DataFrame(columns=[c.name for c in self.columns]),
                PaginationState(has_more=False),
            )

        records = []
        for g in groups:
            records.append({
                "id": g["id"],
                "name": g.get("name"),
                "visibility": g.get("visibility"),
                "default": str(g.get("default", False)).lower(),
                "allows_public_repositories": str(g.get("allows_public_repositories", False)).lower(),
            })
        df = pd.DataFrame(records)

        has_more = len(groups) >= self.pagination_config.page_size
        return df, PaginationState(
            has_more=has_more, next_page=None, total_records=total,
        )
