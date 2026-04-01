"""GitHub Actions workflow jobs — job-level details for each workflow run.

Uses composite entity key (repo_full_name + run id) from github_workflow_runs.
The parent table has primary_key=["id"] but build_entity_request needs the
repo_full_name too, so we use the repo_full_name column from the parent table.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import get_github_base_url
from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import GitHubAppTokenManager


@register
class GitHubWorkflowJobs(APIAsset):
    """Job-level details for each workflow run."""

    name = "github_workflow_jobs"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_workflow_jobs"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="page_number", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_workflow_runs"

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

    def build_entity_request(
        self, entity_key: Any, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        # entity_key is a dict: {"id": run_id, "repo_full_name": "org/repo"}
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

    def build_request(self, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self.build_entity_request({"id": 0, "repo_full_name": "_placeholder"}, context, checkpoint)

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        jobs = response.get("jobs", [])
        total = response.get("total_count", 0)

        if not jobs:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        records = []
        for j in jobs:
            records.append({
                "id": j["id"],
                "run_id": j.get("run_id"),
                "repo_full_name": "",  # Filled from parent data or entity context
                "name": j.get("name"),
                "status": j.get("status"),
                "conclusion": j.get("conclusion"),
                "started_at": j.get("started_at"),
                "completed_at": j.get("completed_at"),
                "runner_name": j.get("runner_name"),
                "runner_group_name": j.get("runner_group_name"),
            })
        df = pd.DataFrame(records)
        has_more = len(jobs) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None, total_records=total)
