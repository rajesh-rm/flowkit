"""GitHub org members — lists all members of a GitHub organization."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import get_github_base_url, get_github_org
from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import GitHubAppTokenManager


@register
class GitHubMembers(APIAsset):
    """Organization members — login, id, type for each member."""

    name = "github_members"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_members"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

    load_strategy = LoadStrategy.UPSERT  # UPSERT so multi-org runs don't wipe each other
    default_run_mode = RunMode.FULL

    columns = [
        Column("login", "TEXT", nullable=False),
        Column("id", "INTEGER"),
        Column("avatar_url", "TEXT"),
        Column("type", "TEXT"),
    ]

    primary_key = ["login"]

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None
    ) -> RequestSpec:
        org = get_github_org()
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()

        return RequestSpec(
            method="GET",
            url=f"{base}/orgs/{org}/members",
            params={"per_page": 100, "page": page},
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
