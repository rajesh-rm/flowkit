"""GitHub custom property values per repository."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import filter_to_current_org, get_github_base_url
from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import GitHubAppTokenManager


@register
class GitHubRepoProperties(APIAsset):
    """Custom property values for each repository."""

    name = "github_repo_properties"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_repo_properties"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="none")
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_repos"
    entity_key_column = "repo_full_name"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("repo_full_name", "TEXT", nullable=False),
        Column("property_name", "TEXT", nullable=False),
        Column("value", "TEXT"),
    ]

    primary_key = ["repo_full_name", "property_name"]

    def filter_entity_keys(self, keys: list) -> list:
        return filter_to_current_org(keys)

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/repos/{entity_key}/properties/values",
            params={},
            headers={"Accept": "application/vnd.github+json"},
        )

    def build_request(self, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self.build_entity_request("_placeholder", context, checkpoint)

    def parse_response(self, response: list[dict[str, Any]]) -> tuple[pd.DataFrame, PaginationState]:
        if not response:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        records = []
        for prop in response:
            records.append({
                "repo_full_name": "",  # Injected by entity_key_column
                "property_name": prop.get("property_name", ""),
                "value": str(prop.get("value", "")),
            })
        df = pd.DataFrame(records)
        return df, PaginationState(has_more=False)
