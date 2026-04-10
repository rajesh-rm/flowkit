"""GitHub Actions workflow jobs — job-level details for each workflow run.

Uses composite entity key (repo_full_name + run id) from github_workflow_runs.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset, get_github_base_url
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec
from sqlalchemy import BigInteger, DateTime, Text


@register
class GitHubWorkflowJobs(GitHubRepoAsset):
    """Job-level details for each workflow run."""

    name = "github_workflow_jobs"
    target_table = "github_workflow_jobs"

    parent_asset_name = "github_workflow_runs"
    entity_key_column = None  # Composite key — handled manually

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    # repo_full_name not included — JOIN to github_workflow_runs via run_id
    columns = [
        Column("id", BigInteger(), nullable=False),
        Column("run_id", BigInteger()),
        Column("name", Text()),
        Column("status", Text()),
        Column("conclusion", Text()),
        Column("started_at", DateTime(timezone=True)),
        Column("completed_at", DateTime(timezone=True)),
        Column("runner_name", Text()),
        Column("runner_group_name", Text()),
    ]
    primary_key = ["id"]
    indexes = [
        Index(columns=("run_id",)),
        Index(columns=("completed_at",)),
        Index(columns=("conclusion",)),
    ]
    date_column = "completed_at"

    def filter_entity_keys(self, keys: list) -> list:
        # No org filtering needed — parent (workflow_runs) is already org-scoped
        return keys

    def build_entity_request(self, entity_key: Any, context: RunContext, checkpoint=None) -> RequestSpec:
        if not isinstance(entity_key, dict):
            raise TypeError(
                f"GitHubWorkflowJobs expects dict entity_key with 'id' and "
                f"'repo_full_name' keys (got {type(entity_key).__name__})"
            )
        run_id = entity_key["id"]
        repo = entity_key["repo_full_name"]

        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/repos/{repo}/actions/runs/{run_id}/jobs",
            params={"per_page": 100, "page": page, "filter": "latest"},
            headers={"Accept": "application/vnd.github+json"},
        )

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        return self._parse_wrapped_response(response, "jobs", lambda j: {
            "id": j["id"],
            "run_id": j.get("run_id"),
            "name": j.get("name"),
            "status": j.get("status"),
            "conclusion": j.get("conclusion"),
            "started_at": j.get("started_at"),
            "completed_at": j.get("completed_at"),
            "runner_name": j.get("runner_name"),
            "runner_group_name": j.get("runner_group_name"),
        })
