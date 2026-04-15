"""GitHub Actions workflows — CI/CD workflow definitions per repository."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec
from sqlalchemy import BigInteger, DateTime, Text


@register
class GitHubWorkflows(GitHubRepoAsset):
    """CI/CD workflow definitions per repository."""

    name = "github_workflows"
    target_table = "github_workflows"

    columns = [
        Column("id", BigInteger(), nullable=False),
        Column("repo_full_name", Text()),
        Column("name", Text()),
        Column("path", Text()),
        Column("state", Text()),
        Column("created_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
    ]
    primary_key = ["id"]
    indexes = [
        Index(columns=("repo_full_name",)),
    ]

    def build_entity_request(self, entity_key: str, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self._paginated_entity_request(
            entity_key, f"/repos/{entity_key}/actions/workflows", checkpoint,
        )

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        return self._parse_wrapped_response(response, "workflows", lambda w: {
            "id": w["id"],
            "repo_full_name": "",
            "name": w.get("name"),
            "path": w.get("path"),
            "state": w.get("state"),
            "created_at": w.get("created_at"),
            "updated_at": w.get("updated_at"),
        })
