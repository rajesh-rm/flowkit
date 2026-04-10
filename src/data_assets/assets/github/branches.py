"""GitHub branches — all branches per repository."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec
from sqlalchemy import Boolean, Text


@register
class GitHubBranches(GitHubRepoAsset):
    """Branches per repository — entity-parallel by repo."""

    name = "github_branches"
    target_table = "github_branches"

    columns = [
        Column("repo_full_name", Text(), nullable=False),
        Column("name", Text(), nullable=False),
        Column("protected", Boolean()),
        Column("commit_sha", Text()),
    ]
    column_max_lengths = {
        "repo_full_name": 200,
        "name": 256,            # branch name
        "commit_sha": 40,       # SHA-1 hex
    }

    primary_key = ["repo_full_name", "name"]
    indexes = [
        Index(columns=("protected",)),
    ]

    def build_entity_request(self, entity_key: str, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self._paginated_entity_request(entity_key, f"/repos/{entity_key}/branches", checkpoint)

    def parse_response(self, response: list[dict[str, Any]]) -> tuple[pd.DataFrame, PaginationState]:
        return self._parse_array_response(response, lambda b: {
            "repo_full_name": "",
            "name": b["name"],
            "protected": b.get("protected", False),
            "commit_sha": b.get("commit", {}).get("sha", ""),
        })
