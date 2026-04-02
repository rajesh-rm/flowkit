"""GitHub user details — full profile for each org member."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import get_github_base_url
from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import GitHubAppTokenManager


@register
class GitHubUserDetails(APIAsset):
    """Full user profile for each organization member."""

    name = "github_user_details"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_user_details"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="none")
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_members"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("login", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("company", "TEXT"),
        Column("location", "TEXT"),
        Column("email", "TEXT"),
        Column("bio", "TEXT"),
        Column("public_repos", "INTEGER"),
        Column("followers", "INTEGER"),
        Column("created_at", "TIMESTAMPTZ"),
        Column("updated_at", "TIMESTAMPTZ"),
    ]
    primary_key = ["login"]

    def build_entity_request(self, entity_key, context: RunContext, checkpoint=None) -> RequestSpec:
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/users/{entity_key}",
            params={},
            headers={"Accept": "application/vnd.github+json"},
        )

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        if not response or "login" not in response:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        row = {
            "login": response["login"],
            "name": response.get("name"),
            "company": response.get("company"),
            "location": response.get("location"),
            "email": response.get("email"),
            "bio": response.get("bio"),
            "public_repos": response.get("public_repos"),
            "followers": response.get("followers"),
            "created_at": response.get("created_at"),
            "updated_at": response.get("updated_at"),
        }
        return pd.DataFrame([row]), PaginationState(has_more=False)
