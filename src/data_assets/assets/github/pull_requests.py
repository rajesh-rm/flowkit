from __future__ import annotations

import os
from typing import Any

import pandas as pd

from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import GitHubAppTokenManager


@register
class GitHubPullRequests(APIAsset):
    """Fetches pull request data for each repository, in parallel by repo."""

    name = "github_pull_requests"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_pull_requests"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_repos"

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("number", "INTEGER"),
        Column("title", "TEXT"),
        Column("state", "TEXT"),
        Column("user_login", "TEXT"),
        Column("repo_full_name", "TEXT"),
        Column("created_at", "TIMESTAMPTZ"),
        Column("updated_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("merged_at", "TIMESTAMPTZ", nullable=True),
        Column("draft", "TEXT"),
        Column("head_ref", "TEXT"),
        Column("base_ref", "TEXT"),
        Column("html_url", "TEXT"),
    ]

    primary_key = ["id"]
    date_column = "updated_at"
    # GitHub PRs endpoint does NOT support a `since` query param.
    # We sort by updated desc and use should_stop() to halt when
    # all PRs on a page are older than the watermark.

    def build_entity_request(
        self,
        entity_key: str,
        context: RunContext,
        checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        page = 1
        if checkpoint:
            page = checkpoint.get("next_page", 1)

        base = os.environ.get("GITHUB_API_URL", self.base_url)

        params: dict[str, Any] = {
            "per_page": 100,
            "page": page,
            "state": "all",
            "sort": "updated",
            "direction": "desc",
        }

        return RequestSpec(
            method="GET",
            url=f"{base}/repos/{entity_key}/pulls",
            params=params,
            headers={"Accept": "application/vnd.github+json"},
        )

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None
    ) -> RequestSpec:
        # Not used directly for entity-parallel assets; delegates to
        # build_entity_request with a placeholder entity key.
        return self.build_entity_request("_placeholder", context, checkpoint)

    def parse_response(
        self, response: list[dict[str, Any]]
    ) -> tuple[pd.DataFrame, PaginationState]:
        if not response:
            return (
                pd.DataFrame(columns=[c.name for c in self.columns]),
                PaginationState(has_more=False),
            )

        records = []
        for pr in response:
            records.append(
                {
                    "id": pr["id"],
                    "number": pr.get("number"),
                    "title": pr.get("title"),
                    "state": pr.get("state"),
                    "user_login": pr.get("user", {}).get("login", ""),
                    "repo_full_name": pr.get("base", {})
                    .get("repo", {})
                    .get("full_name", ""),
                    "created_at": pr.get("created_at"),
                    "updated_at": pr.get("updated_at"),
                    "closed_at": pr.get("closed_at"),
                    "merged_at": pr.get("merged_at"),
                    "draft": str(pr.get("draft", False)).lower(),
                    "head_ref": pr.get("head", {}).get("ref", ""),
                    "base_ref": pr.get("base", {}).get("ref", ""),
                    "html_url": pr.get("html_url", ""),
                }
            )
        df = pd.DataFrame(records)

        has_more = len(response) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None)

    def should_stop(self, df: pd.DataFrame, context: RunContext) -> bool:
        """Stop paginating when all PRs on the page are older than the watermark.

        Since we sort by updated desc, once we hit a full page where every PR
        has updated_at < start_date, there's no point fetching more pages —
        they'll all be older. In FULL mode, never stop early.
        """
        if context.mode.value != "forward" or not context.start_date:
            return False
        if df.empty or "updated_at" not in df.columns:
            return False

        updated = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
        oldest_on_page = updated.min()
        if oldest_on_page is pd.NaT:
            return False

        return oldest_on_page < context.start_date
