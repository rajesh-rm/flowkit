from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.jira.helpers import JiraAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from sqlalchemy import DateTime, Text

_ISSUE_FIELDS = (
    "summary,status,priority,issuetype,assignee,"
    "reporter,created,updated,resolutiondate,labels"
)


@register
class JiraIssues(JiraAsset):
    """Jira issues asset -- fetches issues, optionally scoped per project."""

    name = "jira_issues"
    target_table = "jira_issues"

    pagination_config = PaginationConfig(strategy="offset", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "jira_projects"
    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("id", Text(), nullable=False),
        Column("key", Text(), nullable=False),
        Column("summary", Text()),
        Column("status", Text()),
        Column("priority", Text()),
        Column("issue_type", Text()),
        Column("project_key", Text()),
        Column("assignee", Text(), nullable=True),
        Column("reporter", Text(), nullable=True),
        Column("created", DateTime(timezone=True)),
        Column("updated", DateTime(timezone=True)),
        Column("resolution_date", DateTime(timezone=True), nullable=True),
        Column("labels", Text(), nullable=True),
    ]

    primary_key = ["id"]
    indexes = [
        Index(columns=("key",), unique=True),
        Index(columns=("project_key",)),
        Index(columns=("status",)),
        Index(columns=("updated",)),
        Index(columns=("assignee",)),
    ]
    date_column = "updated"
    api_date_param = "jql"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_jql(
        project_key: str | None = None,
        start_date: str | None = None,
    ) -> str:
        clauses: list[str] = []
        if project_key:
            clauses.append(f'project = "{project_key}"')
        if start_date:
            clauses.append(f'updated >= "{start_date}"')
        jql = " AND ".join(clauses)
        return f"{jql} ORDER BY updated ASC" if jql else "ORDER BY updated ASC"

    # ------------------------------------------------------------------
    # Request building
    # ------------------------------------------------------------------

    def _build_search_request(
        self,
        context: RunContext,
        checkpoint: dict[str, Any] | None,
        project_key: str | None = None,
    ) -> RequestSpec:
        start_date_iso = context.start_date.isoformat() if context.start_date else None
        jql = self._build_jql(project_key=project_key, start_date=start_date_iso)
        start_at = checkpoint.get("next_offset", 0) if checkpoint else 0
        base = self.get_jira_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/rest/api/3/search",
            params={
                "jql": jql,
                "maxResults": 100,
                "startAt": start_at,
                "fields": _ISSUE_FIELDS,
            },
        )

    def build_entity_request(
        self,
        entity_key: str,
        context: RunContext,
        checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        return self._build_search_request(context, checkpoint, project_key=entity_key)

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        return self._build_search_request(context, checkpoint)

    # ------------------------------------------------------------------
    # Response parsing (shared by both modes)
    # ------------------------------------------------------------------

    def parse_response(
        self,
        response: dict[str, Any],
    ) -> tuple[pd.DataFrame, PaginationState]:
        issues = response.get("issues", [])

        records: list[dict[str, Any]] = []
        for issue in issues:
            fields = issue.get("fields", {})
            assignee_field = fields.get("assignee")
            reporter_field = fields.get("reporter")

            records.append(
                {
                    "id": issue.get("id"),
                    "key": issue.get("key"),
                    "summary": fields.get("summary"),
                    "status": (fields.get("status") or {}).get("name", ""),
                    "priority": (fields.get("priority") or {}).get("name", ""),
                    "issue_type": (fields.get("issuetype") or {}).get("name", ""),
                    "project_key": (fields.get("project") or {}).get("key", ""),
                    "assignee": assignee_field.get("displayName") if assignee_field else None,
                    "reporter": reporter_field.get("displayName") if reporter_field else None,
                    "created": fields.get("created"),
                    "updated": fields.get("updated"),
                    "resolution_date": fields.get("resolutiondate"),
                    "labels": ",".join(fields.get("labels", [])) or None,
                }
            )

        df = pd.DataFrame(records, columns=[c.name for c in self.columns])

        total = response.get("total", 0)
        start_at = response.get("startAt", 0)
        fetched = len(issues)
        has_more = (start_at + fetched) < total
        next_offset = start_at + fetched

        return df, PaginationState(
            has_more=has_more,
            next_offset=next_offset,
            total_records=total,
        )
