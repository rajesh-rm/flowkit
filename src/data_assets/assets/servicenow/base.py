"""Base class for ServiceNow Table API assets with keyset pagination.

Both incidents and changes use the same ServiceNow Table API with identical
keyset pagination (sys_updated_on + sys_id). This base class holds the shared
build_request() and parse_response() logic. Subclasses only set table_name
and columns.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pandas as pd

from data_assets.core.api_asset import APIAsset
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import ServiceNowTokenManager


class ServiceNowTableAsset(APIAsset):
    """Base for ServiceNow assets that fetch from the Table API.

    Subclasses must set: name, target_table, table_name, columns.
    """

    source_name = "servicenow"
    target_schema = "raw"

    token_manager_class = ServiceNowTokenManager
    base_url = ""
    rate_limit_per_second = 10.0

    pagination_config = PaginationConfig(strategy="keyset", page_size=100)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    primary_key = ["sys_id"]
    date_column = "sys_updated_on"

    # Subclass must set this to the ServiceNow table name (e.g., "incident")
    table_name: str = ""

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        base = os.environ.get("SERVICENOW_INSTANCE", self.base_url)
        url = f"{base}/api/now/table/{self.table_name}"

        # Keyset pagination: sort by sys_updated_on,sys_id and filter from last seen.
        # ServiceNow encoded query syntax: ^ = AND, ^OR = OR.
        query_parts: list[str] = []

        if checkpoint and checkpoint.get("cursor"):
            raw = checkpoint["cursor"]
            last = json.loads(raw) if isinstance(raw, str) else raw
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
            df = pd.DataFrame(columns=[c.name for c in self.columns])

        has_more = len(results) == self.pagination_config.page_size

        cursor = None
        if results:
            last = results[-1]
            cursor = json.dumps({
                "sys_updated_on": last.get("sys_updated_on", ""),
                "sys_id": last.get("sys_id", ""),
            })

        return df, PaginationState(has_more=has_more, cursor=cursor)
