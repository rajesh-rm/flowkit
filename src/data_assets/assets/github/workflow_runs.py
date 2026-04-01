"""GitHub Actions workflow runs — CI/CD execution history per repository."""

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
class GitHubWorkflowRuns(APIAsset):
    """CI/CD workflow run history per repository."""

    name = "github_workflow_runs"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_workflow_runs"

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

    def filter_entity_keys(self, keys: list) -> list:
        return filter_to_current_org(keys)

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/repos/{entity_key}/actions/runs",
            params={"per_page": 100, "page": page},
            headers={"Accept": "application/vnd.github+json"},
        )

    def build_request(self, context: RunContext, checkpoint: dict | None = None) -> RequestSpec:
        return self.build_entity_request("_placeholder", context, checkpoint)

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        runs = response.get("workflow_runs", [])
        total = response.get("total_count", 0)

        if not runs:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        records = []
        for r in runs:
            records.append({
                "id": r["id"],
                "repo_full_name": "",  # Injected by entity_key_column
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
        df = pd.DataFrame(records)
        has_more = len(runs) >= self.pagination_config.page_size
        return df, PaginationState(has_more=has_more, next_page=None, total_records=total)
