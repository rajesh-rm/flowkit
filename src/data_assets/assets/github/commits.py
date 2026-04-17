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
from sqlalchemy import DateTime, Text


@register
class GitHubCommits(GitHubRepoAsset):
    """Commit history per repository — supports incremental via since param."""

    name = "github_commits"
    target_table = "github_commits"

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("sha", Text(), nullable=False),
        Column("repo_full_name", Text()),
        Column("author_login", Text()),
        Column("author_date", DateTime(timezone=True)),
        Column("committer_login", Text()),
        Column("committer_date", DateTime(timezone=True)),
        Column("message", Text()),
        Column("html_url", Text()),
    ]
    column_max_lengths = {
        "sha": 40,
        "repo_full_name": 200,
        "author_login": 100,
        "committer_login": 100,
        "html_url": 2048,
    }

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
        self._check_required_keys(response, {
            "sha": "sha",
            "author.login": "author_login",
            "commit.author.date": "author_date",
            "committer.login": "committer_login",
            "commit.committer.date": "committer_date",
            "commit.message": "message",
            "html_url": "html_url",
        })
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
