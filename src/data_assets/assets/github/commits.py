"""GitHub commits — commit history per repository with incremental sync."""

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
class GitHubCommits(APIAsset):
    """Commit history per repository — supports incremental via since param."""

    name = "github_commits"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_commits"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_repos"
    entity_key_column = "repo_full_name"

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("sha", "TEXT", nullable=False),
        Column("repo_full_name", "TEXT"),
        Column("author_login", "TEXT"),
        Column("author_date", "TIMESTAMPTZ"),
        Column("committer_login", "TEXT"),
        Column("committer_date", "TIMESTAMPTZ"),
        Column("message", "TEXT"),
        Column("html_url", "TEXT"),
    ]

    primary_key = ["sha"]
    date_column = "committer_date"

    def filter_entity_keys(self, keys: list) -> list:
        return filter_to_current_org(keys)

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()
        params: dict[str, Any] = {
            "per_page": 100,
            "page": page,
        }
        if context.start_date:
            params["since"] = context.start_date.isoformat()
        return RequestSpec(
            method="GET",
            url=f"{base}/repos/{entity_key}/commits",
            params=params,
            headers={"Accept": "application/vnd.github+json"},
        )

    def build_request(self, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self.build_entity_request("_placeholder", context, checkpoint)

    def parse_response(self, response: list[dict[str, Any]]) -> tuple[pd.DataFrame, PaginationState]:
        if not response:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        records = []
        for c in response:
            commit = c.get("commit", {})
            author = commit.get("author", {})
            committer = commit.get("committer", {})
            records.append({
                "sha": c["sha"],
                "repo_full_name": "",  # Injected by entity_key_column
                "author_login": (c.get("author") or {}).get("login", ""),
                "author_date": author.get("date"),
                "committer_login": (c.get("committer") or {}).get("login", ""),
                "committer_date": committer.get("date"),
                "message": commit.get("message", ""),
                "html_url": c.get("html_url", ""),
            })
        df = pd.DataFrame(records)
        has_more = len(response) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None)
