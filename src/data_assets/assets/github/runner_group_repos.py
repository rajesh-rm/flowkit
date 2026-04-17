"""GitHub runner group repository assignments."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import (
    GitHubRepoAsset,
    get_github_base_url,
    get_github_org,
)
from data_assets.core.column import Column, Index
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec
from sqlalchemy import BigInteger, Text


@register
class GitHubRunnerGroupRepos(GitHubRepoAsset):
    """Repositories assigned to each self-hosted runner group."""

    name = "github_runner_group_repos"
    target_table = "github_runner_group_repos"

    max_workers = 3

    parent_asset_name = "github_runner_groups"
    entity_key_column = "runner_group_id"

    columns = [
        Column("runner_group_id", BigInteger(), nullable=False),
        Column("repo_id", BigInteger(), nullable=False),
        Column("repo_full_name", Text()),
    ]
    primary_key = ["runner_group_id", "repo_id"]
    indexes = [
        Index(columns=("repo_full_name",)),
    ]

    def filter_entity_keys(self, keys: list) -> list:
        return keys  # No org filtering — parent is already org-scoped

    def build_entity_request(self, entity_key: Any, context: RunContext, checkpoint=None) -> RequestSpec:
        org = get_github_org()
        return self._paginated_entity_request(
            entity_key,
            f"/orgs/{org}/actions/runner-groups/{entity_key}/repositories",
            checkpoint,
        )

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        self._check_required_keys(response.get("repositories", []), {
            "id": "repo_id",
            "full_name": "repo_full_name",
        })
        return self._parse_wrapped_response(response, "repositories", lambda r: {
            "runner_group_id": 0,
            "repo_id": r["id"],
            "repo_full_name": r.get("full_name", ""),
        })
