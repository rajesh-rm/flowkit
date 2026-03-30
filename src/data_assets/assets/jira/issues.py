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

_ISSUE_FIELDS = (
    "summary,status,priority,issuetype,assignee,"
    "reporter,created,updated,resolutiondate,labels"
)


@register
class JiraIssues(APIAsset):
    """Jira issues asset -- fetches issues, optionally scoped per project."""

    name = "jira_issues"
    source_name = "jira"
    target_schema = "raw"
    target_table = "jira_issues"

    token_manager_class = JiraTokenManager
    base_url: str = os.environ.get("JIRA_URL", "")
    rate_limit_per_second = 5.0

    pagination_config = PaginationConfig(strategy="offset", page_size=100)
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "jira_projects"
    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("id", "TEXT", nullable=False),
        Column("key", "TEXT", nullable=False),
        Column("summary", "TEXT"),
        Column("status", "TEXT"),
        Column("priority", "TEXT"),
        Column("issue_type", "TEXT"),
        Column("project_key", "TEXT"),
        Column("assignee", "TEXT", nullable=True),
        Column("reporter", "TEXT", nullable=True),
        Column("created", "TIMESTAMPTZ"),
        Column("updated", "TIMESTAMPTZ"),
        Column("resolution_date", "TIMESTAMPTZ", nullable=True),
        Column("labels", "TEXT", nullable=True),
    ]

    primary_key = ["id"]
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
        return " AND ".join(clauses) if clauses else "ORDER BY updated ASC"

    # ------------------------------------------------------------------
    # Entity-parallel request (one project at a time)
    # ------------------------------------------------------------------

    def build_entity_request(
        self,
        entity_key: str,
        context: RunContext,
        checkpoint: dict[str, Any] | None,
    ) -> RequestSpec:
        start_date_iso: str | None = None
        if context.start_date:
            start_date_iso = context.start_date.isoformat()

        jql = self._build_jql(
            project_key=entity_key,
            start_date=start_date_iso,
        )

        start_at = checkpoint.get("next_offset", 0) if checkpoint else 0

        return RequestSpec(
            method="GET",
            url=f"{self.base_url}/rest/api/3/search",
            params={
                "jql": jql,
                "maxResults": 100,
                "startAt": start_at,
                "fields": _ISSUE_FIELDS,
            },
        )

    # ------------------------------------------------------------------
    # Fallback full request (no project scoping)
    # ------------------------------------------------------------------

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict[str, Any] | None,
    ) -> RequestSpec:
        start_date_iso: str | None = None
        if context.start_date:
            start_date_iso = context.start_date.isoformat()

        jql = self._build_jql(start_date=start_date_iso)
        start_at = checkpoint.get("next_offset", 0) if checkpoint else 0

        return RequestSpec(
            method="GET",
            url=f"{self.base_url}/rest/api/3/search",
            params={
                "jql": jql,
                "maxResults": 100,
                "startAt": start_at,
                "fields": _ISSUE_FIELDS,
            },
        )

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
