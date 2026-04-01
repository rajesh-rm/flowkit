"""GitHub Actions workflow jobs — job-level details for each workflow run.

Uses composite entity key (repo_full_name + run id) from github_workflow_runs.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import GitHubRepoAsset, get_github_base_url
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec


@register
class GitHubWorkflowJobs(GitHubRepoAsset):
    """Job-level details for each workflow run."""

    name = "github_workflow_jobs"
    target_table = "github_workflow_jobs"

    parent_asset_name = "github_workflow_runs"
    entity_key_column = None  # Composite key — handled manually

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("id", "BIGINT", nullable=False),
        Column("run_id", "BIGINT"),
        Column("repo_full_name", "TEXT"),
        Column("name", "TEXT"),
        Column("status", "TEXT"),
        Column("conclusion", "TEXT"),
        Column("started_at", "TIMESTAMPTZ"),
        Column("completed_at", "TIMESTAMPTZ"),
        Column("runner_name", "TEXT"),
        Column("runner_group_name", "TEXT"),
    ]
    primary_key = ["id"]
    date_column = "completed_at"

    def filter_entity_keys(self, keys: list) -> list:
        # No org filtering needed — parent (workflow_runs) is already org-scoped
        return keys

    def build_entity_request(self, entity_key: Any, context: RunContext, checkpoint=None) -> RequestSpec:
        if isinstance(entity_key, dict):
            run_id = entity_key["id"]
            repo = entity_key["repo_full_name"]
        else:
            run_id = entity_key
            repo = "_unknown"

        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/repos/{repo}/actions/runs/{run_id}/jobs",
            params={"per_page": 100, "page": page, "filter": "latest"},
            headers={"Accept": "application/vnd.github+json"},
        )

    def build_request(self, context: RunContext, checkpoint=None) -> RequestSpec:
        return self.build_entity_request({"id": 0, "repo_full_name": "_placeholder"}, context, checkpoint)

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        return self._parse_wrapped_response(response, "jobs", lambda j: {
            "id": j["id"],
            "run_id": j.get("run_id"),
            "repo_full_name": "",
            "name": j.get("name"),
            "status": j.get("status"),
            "conclusion": j.get("conclusion"),
            "started_at": j.get("started_at"),
            "completed_at": j.get("completed_at"),
            "runner_name": j.get("runner_name"),
            "runner_group_name": j.get("runner_group_name"),
        })
