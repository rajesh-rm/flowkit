"""GitHub Actions workflow runs — CI/CD execution history per repository."""

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
class GitHubWorkflowRuns(GitHubRepoAsset):
    """CI/CD workflow run history per repository."""

    name = "github_workflow_runs"
    target_table = "github_workflow_runs"

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("id", "BIGINT", nullable=False),
        Column("repo_full_name", "TEXT"),
        Column("name", "TEXT"),
        Column("workflow_id", "INTEGER"),
        Column("status", "TEXT"),
        Column("conclusion", "TEXT"),
        Column("head_branch", "TEXT"),
        Column("head_sha", "TEXT"),
        Column("event", "TEXT"),
        Column("run_number", "INTEGER"),
        Column("run_attempt", "INTEGER"),
        Column("created_at", "TIMESTAMPTZ"),
        Column("updated_at", "TIMESTAMPTZ"),
        Column("run_started_at", "TIMESTAMPTZ"),
        Column("html_url", "TEXT"),
    ]
    primary_key = ["id"]
    date_column = "updated_at"

    def build_entity_request(self, entity_key: str, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self._paginated_entity_request(
            entity_key, f"/repos/{entity_key}/actions/runs", checkpoint,
        )

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        return self._parse_wrapped_response(response, "workflow_runs", lambda r: {
            "id": r["id"],
            "repo_full_name": "",
            "name": r.get("name"),
            "workflow_id": r.get("workflow_id"),
            "status": r.get("status"),
            "conclusion": r.get("conclusion"),
            "head_branch": r.get("head_branch"),
            "head_sha": r.get("head_sha"),
            "event": r.get("event"),
            "run_number": r.get("run_number"),
            "run_attempt": r.get("run_attempt"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            "run_started_at": r.get("run_started_at"),
            "html_url": r.get("html_url"),
        })
