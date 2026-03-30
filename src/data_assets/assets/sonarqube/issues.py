"""SonarQube issues — sorted by update_date for reliable incremental sync.

SonarQube's /api/issues/search only supports `createdAfter` (creation date),
which misses updates to existing issues (resolved, reopened, severity changes).

Instead, we sort by UPDATE_DATE ascending and use should_stop() to halt when
we've passed the watermark. This captures all changes, not just new issues.
"""

from __future__ import annotations

import math
import os

import pandas as pd

from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import SonarQubeTokenManager


@register
class SonarQubeIssues(APIAsset):
    """SonarQube issues — captures new AND updated issues via update_date sort."""

    name = "sonarqube_issues"
    source_name = "sonarqube"

    target_schema = "raw"
    target_table = "sonarqube_issues"

    token_manager_class = SonarQubeTokenManager
    base_url = ""

    rate_limit_per_second = 5.0

    pagination_config = PaginationConfig(
        strategy="page_number",
        page_size=100,
        total_field="paging.total",
    )

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_projects"

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("key", "TEXT", nullable=False),
        Column("rule", "TEXT"),
        Column("severity", "TEXT"),
        Column("component", "TEXT"),
        Column("project", "TEXT"),
        Column("line", "INTEGER", nullable=True),
        Column("message", "TEXT"),
        Column("status", "TEXT"),
        Column("type", "TEXT"),
        Column("creation_date", "TIMESTAMPTZ"),
        Column("update_date", "TIMESTAMPTZ"),
    ]

    primary_key = ["key"]
    date_column = "update_date"  # Track watermark on update_date, not creation_date

    def build_entity_request(
        self,
        entity_key: str,
        context: RunContext,
        checkpoint: dict | None = None,
    ) -> RequestSpec:
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        params: dict = {
            "componentKeys": entity_key,
            "ps": 100,
            "p": page,
            "s": "UPDATE_DATE",  # Sort by update date for reliable incremental
            "asc": "true",       # Ascending so oldest updates come first
        }

        base = os.environ.get("SONARQUBE_URL", self.base_url)
        return RequestSpec(
            url=f"{base}/api/issues/search",
            method="GET",
            params=params,
        )

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict | None = None,
    ) -> RequestSpec:
        # Entity-parallel asset — build_entity_request is the real entry point.
        # This satisfies the abstract method contract.
        return self.build_entity_request("_all", context, checkpoint)

    def parse_response(
        self,
        response: dict,
    ) -> tuple[pd.DataFrame, PaginationState]:
        paging = response["paging"]
        total = paging["total"]
        page_index = paging["pageIndex"]
        page_size = paging["pageSize"]

        total_pages = math.ceil(total / page_size) if page_size else 1

        valid_columns = {c.name for c in self.columns}
        rename_map = {
            "creationDate": "creation_date",
            "updateDate": "update_date",
        }

        df = pd.DataFrame(response["issues"])
        df = df.rename(columns=rename_map)
        keep = [c for c in df.columns if c in valid_columns]
        df = df[keep]

        return df, PaginationState(
            has_more=page_index < total_pages,
            next_page=page_index + 1,
            total_pages=total_pages,
            total_records=total,
        )

    # No should_stop() override needed — SonarQube returns paging.total,
    # so page-number pagination exhausts naturally without early termination.
