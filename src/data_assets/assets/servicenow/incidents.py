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
from data_assets.extract.token_manager import ServiceNowTokenManager


@register
class ServiceNowIncidents(APIAsset):
    """ServiceNow incidents fetched via the Table API."""

    name = "servicenow_incidents"
    source_name = "servicenow"
    target_schema = "raw"
    target_table = "servicenow_incidents"

    token_manager_class = ServiceNowTokenManager
    base_url = os.environ.get("SERVICENOW_INSTANCE", "")
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="offset", page_size=100)
    parallel_mode = ParallelMode.PAGE_PARALLEL
    max_workers = 3

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("description", "TEXT"),
        Column("state", "TEXT"),
        Column("priority", "TEXT"),
        Column("severity", "TEXT"),
        Column("category", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("assignment_group", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]

    primary_key = ["sys_id"]
    date_column = "sys_updated_on"
    api_date_param = "sysparm_query"

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict[str, Any] | None,
    ) -> RequestSpec:
        url = f"{self.base_url}/api/now/table/incident"
        offset = checkpoint.get("next_offset", 0) if checkpoint else 0

        params: dict[str, Any] = {
            "sysparm_limit": self.pagination_config.page_size,
            "sysparm_offset": offset,
        }

        if context.start_date:
            start_iso = context.start_date.isoformat()
            params["sysparm_query"] = f"sys_updated_on>={start_iso}"

        return RequestSpec(method="GET", url=url, params=params)

    def parse_response(
        self,
        response: dict[str, Any],
    ) -> tuple[pd.DataFrame, PaginationState]:
        results: list[dict[str, Any]] = response.get("result", [])
        page_size = self.pagination_config.page_size

        if results:
            df = pd.DataFrame(results)
            # Keep only the columns defined on this asset.
            column_names = [c.name for c in self.columns]
            df = df[[c for c in column_names if c in df.columns]]
        else:
            df = pd.DataFrame()

        current_offset = len(results)
        has_more = len(results) == page_size

        return df, PaginationState(
            has_more=has_more,
            next_offset=(
                (response.get("_pagination_offset", 0) or 0) + current_offset
            ),
            total_records=response.get("_total_count"),
        )
