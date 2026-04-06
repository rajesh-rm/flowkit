"""GitHub commits — commit history per repository with incremental sync."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec


@register
class GitHubCommits(GitHubRepoAsset):
    """Commit history per repository — supports incremental via since param."""

    name = "github_commits"
    target_table = "github_commits"

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
    indexes = [
        Index(columns=("repo_full_name",)),
        Index(columns=("committer_date",)),
        Index(columns=("author_login",)),
    ]
    date_column = "committer_date"

    def build_entity_request(self, entity_key: str, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        extra: dict[str, Any] = {}
        if context.start_date:
            extra["since"] = context.start_date.isoformat()
        return self._paginated_entity_request(
            entity_key, f"/repos/{entity_key}/commits", checkpoint, extra,
        )

    def parse_response(self, response: list[dict[str, Any]]) -> tuple[pd.DataFrame, PaginationState]:
        return self._parse_array_response(response, lambda c: {
            "sha": c["sha"],
            "repo_full_name": "",
            "author_login": (c.get("author") or {}).get("login", ""),
            "author_date": c.get("commit", {}).get("author", {}).get("date"),
            "committer_login": (c.get("committer") or {}).get("login", ""),
            "committer_date": c.get("commit", {}).get("committer", {}).get("date"),
            "message": c.get("commit", {}).get("message", ""),
            "html_url": c.get("html_url", ""),
        })
