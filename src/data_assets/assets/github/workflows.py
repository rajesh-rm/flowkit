"""GitHub Actions workflows — CI/CD workflow definitions per repository."""

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
class GitHubWorkflows(APIAsset):
    """CI/CD workflow definitions per repository."""

    name = "github_workflows"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_workflows"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_repos"
    entity_key_column = "repo_full_name"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("repo_full_name", "TEXT"),
        Column("name", "TEXT"),
        Column("path", "TEXT"),
        Column("state", "TEXT"),
        Column("created_at", "TIMESTAMPTZ"),
        Column("updated_at", "TIMESTAMPTZ"),
    ]

    primary_key = ["id"]

    def filter_entity_keys(self, keys: list) -> list:
        return filter_to_current_org(keys)

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/repos/{entity_key}/actions/workflows",
            params={"per_page": 100, "page": page},
            headers={"Accept": "application/vnd.github+json"},
        )

    def build_request(self, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self.build_entity_request("_placeholder", context, checkpoint)

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        workflows = response.get("workflows", [])
        total = response.get("total_count", 0)

        if not workflows:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        records = []
        for w in workflows:
            records.append({
                "id": w["id"],
                "repo_full_name": "",  # Injected by entity_key_column
                "name": w.get("name"),
                "path": w.get("path"),
                "state": w.get("state"),
                "created_at": w.get("created_at"),
                "updated_at": w.get("updated_at"),
            })
        df = pd.DataFrame(records)
        has_more = len(workflows) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None, total_records=total)
