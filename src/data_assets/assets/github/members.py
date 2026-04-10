"""GitHub org members — lists all members of a GitHub organization."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubOrgAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register
from data_assets.core.types import PaginationState
from sqlalchemy import Integer, Text


@register
class GitHubMembers(GitHubOrgAsset):
    """Organization members — login, id, type for each member."""

    name = "github_members"
    target_table = "github_members"
    org_endpoint = "/members"

    columns = [
        Column("login", Text(), nullable=False),
        Column("id", Integer()),
        Column("avatar_url", Text()),
        Column("type", Text()),
    ]

    column_max_lengths = {
        "login": 100,
        "avatar_url": 2048,
        "type": 100,
    }

    primary_key = ["login"]
    indexes = [
        Index(columns=("id",), unique=True),
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
        for member in response:
            records.append({
                "login": member["login"],
                "id": member.get("id"),
                "avatar_url": member.get("avatar_url"),
                "type": member.get("type"),
            })
        df = pd.DataFrame(records)

        has_more = len(response) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None)
