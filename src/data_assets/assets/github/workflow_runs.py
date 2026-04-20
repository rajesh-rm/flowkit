"""GitHub Actions workflow runs — CI/CD execution history per repository."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec
from sqlalchemy import BigInteger, DateTime, Integer, Text


@register
class GitHubWorkflowRuns(GitHubRepoAsset):
    """CI/CD workflow run history per repository."""

    name = "github_workflow_runs"
    target_table = "github_workflow_runs"

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("id", BigInteger(), nullable=False),
        Column("repo_full_name", Text()),
        Column("name", Text()),
        Column("workflow_id", BigInteger()),
        Column("status", Text()),
        Column("conclusion", Text()),
        Column("head_branch", Text()),
        Column("head_sha", Text()),
        Column("event", Text()),
        Column("run_number", Integer()),
        Column("run_attempt", Integer()),
        Column("created_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
        Column("run_started_at", DateTime(timezone=True)),
        Column("html_url", Text()),
    ]
    column_max_lengths = {
        "repo_full_name": 200,
        "name": 1024,
        "status": 100,
        "conclusion": 100,
        "head_branch": 256,
        "head_sha": 40,
        "event": 100,
        "html_url": 2048,
    }

    primary_key = ["id"]
    indexes = [
        Index(columns=("repo_full_name",)),
        Index(columns=("updated_at",)),
        Index(columns=("workflow_id",)),
        Index(columns=("conclusion",)),
    ]
    date_column = "updated_at"

    def build_entity_request(self, entity_key: str, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self._paginated_entity_request(
            entity_key, f"/repos/{entity_key}/actions/runs", checkpoint,
        )

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        self._check_required_keys(response.get("workflow_runs", []), {
            "id": "id",
            "name": "name",
            "workflow_id": "workflow_id",
            "status": "status",
            "conclusion": "conclusion",
            "head_branch": "head_branch",
            "head_sha": "head_sha",
            "event": "event",
            "run_number": "run_number",
            "run_attempt": "run_attempt",
            "created_at": "created_at",
            "updated_at": "updated_at",
            "run_started_at": "run_started_at",
            "html_url": "html_url",
        })
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
