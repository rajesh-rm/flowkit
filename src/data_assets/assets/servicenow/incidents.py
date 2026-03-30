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
    """ServiceNow incidents fetched via the Table API.

    Uses keyset pagination (sys_updated_on + sys_id) for reliable
    extraction from large tables. Offset pagination is unreliable
    when data changes between pages.
    """

    name = "servicenow_incidents"
    source_name = "servicenow"
    target_schema = "raw"
    target_table = "servicenow_incidents"

    token_manager_class = ServiceNowTokenManager
    base_url = ""
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="keyset", page_size=100)
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

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        base = os.environ.get("SERVICENOW_INSTANCE", self.base_url)
        url = f"{base}/api/now/table/incident"

        # Keyset pagination: sort by sys_updated_on,sys_id and filter from last seen
        query_parts: list[str] = []

        if checkpoint and checkpoint.get("cursor"):
            import json
            last = json.loads(checkpoint["cursor"]) if isinstance(checkpoint["cursor"], str) else checkpoint["cursor"]
            # Records after the last-seen (sys_updated_on, sys_id) pair
            query_parts.append(
                f"sys_updated_on>={last['sys_updated_on']}"
                f"^sys_id>{last['sys_id']}"
                f"^ORsys_updated_on>{last['sys_updated_on']}"
            )
        elif context.start_date:
            query_parts.append(f"sys_updated_on>={context.start_date.isoformat()}")

        params: dict[str, Any] = {
            "sysparm_limit": self.pagination_config.page_size,
            "sysparm_orderby": "sys_updated_on,sys_id",
        }
        if query_parts:
            params["sysparm_query"] = "^".join(query_parts)

        return RequestSpec(method="GET", url=url, params=params)

    def parse_response(
        self, response: dict[str, Any],
    ) -> tuple[pd.DataFrame, PaginationState]:
        results: list[dict[str, Any]] = response.get("result", [])

        if results:
            df = pd.DataFrame(results)
            column_names = [c.name for c in self.columns]
            df = df[[c for c in column_names if c in df.columns]]
        else:
            df = pd.DataFrame()

        has_more = len(results) == self.pagination_config.page_size

        # Keyset cursor: encode last record's sort keys as JSON string
        cursor = None
        if results:
            import json
            last = results[-1]
            cursor = json.dumps({
                "sys_updated_on": last.get("sys_updated_on", ""),
                "sys_id": last.get("sys_id", ""),
            })

        return df, PaginationState(has_more=has_more, cursor=cursor)
