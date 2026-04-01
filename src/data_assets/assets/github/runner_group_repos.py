"""GitHub runner group repository assignments."""

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
class GitHubRunnerGroupRepos(APIAsset):
    """Repositories assigned to each self-hosted runner group."""

    name = "github_runner_group_repos"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_runner_group_repos"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "github_runner_groups"
    entity_key_column = "runner_group_id"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("runner_group_id", "INTEGER", nullable=False),
        Column("repo_id", "INTEGER", nullable=False),
        Column("repo_full_name", "TEXT"),
    ]

    primary_key = ["runner_group_id", "repo_id"]

    def build_entity_request(
        self, entity_key: Any, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        org = get_github_org()
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/orgs/{org}/actions/runner-groups/{entity_key}/repositories",
            params={"per_page": 100, "page": page},
            headers={"Accept": "application/vnd.github+json"},
        )

    def build_request(self, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self.build_entity_request(0, context, checkpoint)

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        repos = response.get("repositories", [])
        total = response.get("total_count", 0)

        if not repos:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        records = []
        for r in repos:
            records.append({
                "runner_group_id": 0,  # Injected by entity_key_column
                "repo_id": r["id"],
                "repo_full_name": r.get("full_name", ""),
            })
        df = pd.DataFrame(records)
        has_more = len(repos) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None, total_records=total)
