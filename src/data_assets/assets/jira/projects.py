from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.jira.helpers import JiraAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec


@register
class JiraProjects(JiraAsset):
    """Jira projects asset -- fetches all projects from the Jira instance."""

    name = "jira_projects"
    target_table = "jira_projects"

    pagination_config = PaginationConfig(strategy="offset", page_size=50)
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

    # PK is "key" (not "id") because jira_issues uses project key in JQL
    # and entity-parallel fans out by primary key values.
    primary_key = ["key"]
    indexes = [
        Index(columns=("name",)),
    ]

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        start_at = checkpoint.get("next_offset", 0) if checkpoint else 0
        return RequestSpec(
            method="GET",
            url=f"{self.get_jira_url()}/rest/api/3/project/search",
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
                "is_private": str(proj.get("isPrivate", False)).lower(),
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
