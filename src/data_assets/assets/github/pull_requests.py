"""GitHub pull requests — per-repository, entity-parallel with watermark-based early stop."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec


@register
class GitHubPullRequests(GitHubRepoAsset):
    """Pull requests per repository — incremental via watermark-based early stop.

    GitHub PRs endpoint doesn't support a `since` param, so we sort by
    updated desc and use should_stop() to halt when all PRs on a page
    are older than the watermark.
    """

    name = "github_pull_requests"
    target_table = "github_pull_requests"

    # PRs extract repo_full_name from the response (base.repo.full_name),
    # so entity_key_column injection is not needed.
    entity_key_column = None

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

    def build_entity_request(
        self,
        entity_key: str,
        context: RunContext,
        checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        return self._paginated_entity_request(
            entity_key, f"/repos/{entity_key}/pulls", checkpoint,
            extra_params={"state": "all", "sort": "updated", "direction": "desc"},
        )

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
            records.append({
                "id": pr["id"],
                "number": pr.get("number"),
                "title": pr.get("title"),
                "state": pr.get("state"),
                "user_login": (pr.get("user") or {}).get("login", ""),
                "repo_full_name": ((pr.get("base") or {}).get("repo") or {}).get("full_name", ""),
                "created_at": pr.get("created_at"),
                "updated_at": pr.get("updated_at"),
                "closed_at": pr.get("closed_at"),
                "merged_at": pr.get("merged_at"),
                "draft": str(pr.get("draft", False)).lower(),
                "head_ref": (pr.get("head") or {}).get("ref", ""),
                "base_ref": (pr.get("base") or {}).get("ref", ""),
                "html_url": pr.get("html_url", ""),
            })
        df = pd.DataFrame(records)

        has_more = len(response) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None)

    def should_stop(self, df: pd.DataFrame, context: RunContext) -> bool:
        """Stop paginating when all PRs on the page are older than the watermark.

        Since we sort by updated desc, once every PR on a page has
        updated_at < start_date, there's no point fetching more pages.
        """
        if context.mode != RunMode.FORWARD or not context.start_date:
            return False
        if df.empty or "updated_at" not in df.columns:
            return False

        updated = pd.to_datetime(df["updated_at"], utc=True, errors="coerce")
        oldest_on_page = updated.min()
        if oldest_on_page is pd.NaT:
            return False

        return oldest_on_page < context.start_date
