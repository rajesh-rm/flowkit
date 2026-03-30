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
    base_url = ""  # Set from SERVICENOW_INSTANCE env var at runtime
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="offset", page_size=100)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

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

    _current_offset: int = 0  # Tracks the offset sent in the last request

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        base = os.environ.get("SERVICENOW_INSTANCE", self.base_url)
        url = f"{base}/api/now/table/incident"
        offset = checkpoint.get("next_offset") if checkpoint else None
        offset = offset if offset is not None else 0
        self._current_offset = offset

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
            column_names = [c.name for c in self.columns]
            df = df[[c for c in column_names if c in df.columns]]
        else:
            df = pd.DataFrame()

        has_more = len(results) == page_size
        next_offset = self._current_offset + len(results)

        return df, PaginationState(
            has_more=has_more,
            next_offset=next_offset,
        )
