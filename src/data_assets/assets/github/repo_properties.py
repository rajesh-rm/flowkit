"""GitHub custom property values per repository."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset
from data_assets.core.column import Column
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec


@register
class GitHubRepoProperties(GitHubRepoAsset):
    """Custom property values for each repository."""

    name = "github_repo_properties"
    target_table = "github_repo_properties"

    pagination_config = PaginationConfig(strategy="none")

    columns = [
        Column("repo_full_name", "TEXT", nullable=False),
        Column("property_name", "TEXT", nullable=False),
        Column("value", "TEXT"),
    ]
    primary_key = ["repo_full_name", "property_name"]

    def build_entity_request(self, entity_key: str, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self._paginated_entity_request(
            entity_key, f"/repos/{entity_key}/properties/values", checkpoint,
        )

    def parse_response(self, response: list[dict[str, Any]]) -> tuple[pd.DataFrame, PaginationState]:
        return self._parse_array_response(response, lambda p: {
            "repo_full_name": "",
            "property_name": p.get("property_name", ""),
            "value": str(p.get("value", "")),
        })
