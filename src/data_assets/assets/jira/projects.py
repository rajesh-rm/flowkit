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
from data_assets.extract.token_manager import JiraTokenManager


@register
class JiraProjects(APIAsset):
    """Jira projects asset -- fetches all projects from the Jira instance."""

    name = "jira_projects"
    source_name = "jira"
    target_schema = "raw"
    target_table = "jira_projects"

    token_manager_class = JiraTokenManager
    base_url = ""  # Set from JIRA_URL env var at runtime
    rate_limit_per_second = 5.0

    pagination_config = PaginationConfig(strategy="offset", page_size=50)
    parallel_mode = ParallelMode.NONE
    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("id", "TEXT", nullable=False),
        Column("key", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("project_type_key", "TEXT"),
        Column("style", "TEXT"),
        Column("is_private", "TEXT"),
    ]

    primary_key = ["key"]

    # ------------------------------------------------------------------
    # Request / response
    # ------------------------------------------------------------------

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict[str, Any] | None,
    ) -> RequestSpec:
        start_at = checkpoint.get("next_offset", 0) if checkpoint else 0
        base = os.environ.get("JIRA_URL", self.base_url)
        return RequestSpec(
            method="GET",
            url=f"{base}/rest/api/3/project/search",
            params={"maxResults": 50, "startAt": start_at},
        )

    def parse_response(
        self,
        response: dict[str, Any],
    ) -> tuple[pd.DataFrame, PaginationState]:
        values = response.get("values", [])

        records = [
            {
                "id": proj.get("id"),
                "key": proj.get("key"),
                "name": proj.get("name"),
                "project_type_key": proj.get("projectTypeKey"),
                "style": proj.get("style"),
                "is_private": str(proj.get("isPrivate", "")),
            }
            for proj in values
        ]

        df = pd.DataFrame(records, columns=[c.name for c in self.columns])

        has_more = not response.get("isLast", True)
        next_offset = response.get("startAt", 0) + len(values)

        return df, PaginationState(
            has_more=has_more,
            next_offset=next_offset,
            total_records=response.get("total"),
        )
