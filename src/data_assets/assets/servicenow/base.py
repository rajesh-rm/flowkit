"""Base class for ServiceNow Table API assets using pysnc (GlideRecord).

All ServiceNow assets share this base. Subclasses only set table_name and
columns. Extraction uses pysnc's GlideRecord with automatic pagination,
retry, and auth — bypassing the httpx API pipeline entirely.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from data_assets.core.api_asset import APIAsset
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.load.loader import write_to_temp

logger = logging.getLogger(__name__)


class ServiceNowTableAsset(APIAsset):
    """Base for ServiceNow assets using pysnc GlideRecord.

    Subclasses must set: name, target_table, table_name, columns.
    """

    source_name = "servicenow"
    target_schema = "raw"

    pagination_config = PaginationConfig(strategy="keyset", page_size=1000)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    primary_key = ["sys_id"]
    date_column = "sys_updated_on"

    # Subclass must set this to the ServiceNow table name (e.g., "incident")
    table_name: str = ""

    # -----------------------------------------------------------------------
    # pysnc-based extraction (bypasses APIClient/httpx pipeline)
    # -----------------------------------------------------------------------

    def _create_pysnc_client(self):
        """Create an authenticated pysnc ServiceNowClient from env vars."""
        from pysnc import ServiceNowClient

        instance = os.environ.get("SERVICENOW_INSTANCE")
        if not instance:
            raise RuntimeError("SERVICENOW_INSTANCE environment variable is required")

        username = os.environ.get("SERVICENOW_USERNAME")
        password = os.environ.get("SERVICENOW_PASSWORD")
        client_id = os.environ.get("SERVICENOW_CLIENT_ID")
        client_secret = os.environ.get("SERVICENOW_CLIENT_SECRET")

        if username and password and client_id and client_secret:
            from pysnc.auth import ServiceNowPasswordGrantFlow

            auth = ServiceNowPasswordGrantFlow(
                username, password, client_id, client_secret,
            )
        elif username and password:
            auth = (username, password)
        else:
            raise RuntimeError(
                "ServiceNow requires SERVICENOW_USERNAME + SERVICENOW_PASSWORD "
                "(with optional SERVICENOW_CLIENT_ID + SERVICENOW_CLIENT_SECRET "
                "for OAuth2)"
            )

        return ServiceNowClient(instance, auth)

    def extract(
        self, engine: Engine, temp_table: str, context: RunContext,
    ) -> int:
        """Extract data via pysnc GlideRecord with automatic pagination."""
        client = self._create_pysnc_client()
        batch_size = self.pagination_config.page_size

        gr = client.GlideRecord(self.table_name, batch_size=batch_size)
        gr.fields = [c.name for c in self.columns]
        gr.order_by("sys_updated_on")

        if context.start_date:
            gr.add_query(
                "sys_updated_on", ">=",
                context.start_date.strftime("%Y-%m-%d %H:%M:%S"),
            )

        gr.query()

        total_rows = 0
        batch: list[dict] = []
        start_time = time.monotonic()
        last_log_time = start_time

        for record in gr:
            batch.append(record.serialize())

            if len(batch) >= batch_size:
                df = self._batch_to_df(batch)
                total_rows += write_to_temp(engine, temp_table, df)
                batch = []

                now = time.monotonic()
                if now - last_log_time >= 30.0:
                    elapsed = now - start_time
                    logger.info(
                        "Progress [%s]: %d rows (%.0fs elapsed)",
                        self.name, total_rows, elapsed,
                    )
                    last_log_time = now

        if batch:
            df = self._batch_to_df(batch)
            total_rows += write_to_temp(engine, temp_table, df)

        logger.info(
            "Extraction complete [%s]: %d rows in %.1fs",
            self.name, total_rows, time.monotonic() - start_time,
        )
        return total_rows

    def _batch_to_df(self, batch: list[dict]) -> pd.DataFrame:
        """Convert a batch of serialized records to a DataFrame with declared columns."""
        df = pd.DataFrame(batch)
        column_names = [c.name for c in self.columns]
        return df[[c for c in column_names if c in df.columns]]

    # -----------------------------------------------------------------------
    # Direct Table API methods (satisfy @abstractmethod, used in unit tests)
    # -----------------------------------------------------------------------

    def build_request(
        self, context: RunContext, checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        """Build a Table API request spec for unit testing and diagnostics."""
        base = os.environ.get("SERVICENOW_INSTANCE", "")
        url = f"{base}/api/now/table/{self.table_name}"

        query: str | None = None

        if checkpoint and checkpoint.get("cursor"):
            raw = checkpoint["cursor"]
            last = json.loads(raw) if isinstance(raw, str) else raw
            query = (
                f"sys_updated_on>={last['sys_updated_on']}"
                f"^sys_id>{last['sys_id']}"
                f"^ORsys_updated_on>{last['sys_updated_on']}"
            )
        elif context.start_date:
            query = f"sys_updated_on>={context.start_date.isoformat()}"

        params: dict[str, Any] = {
            "sysparm_limit": self.pagination_config.page_size,
            "sysparm_orderby": "sys_updated_on,sys_id",
            "sysparm_fields": ",".join(c.name for c in self.columns),
            "sysparm_exclude_reference_link": "true",
            "sysparm_no_count": "true",
        }
        if query:
            params["sysparm_query"] = query

        return RequestSpec(method="GET", url=url, params=params)

    def parse_response(
        self, response: dict[str, Any],
    ) -> tuple[pd.DataFrame, PaginationState]:
        """Parse a Table API JSON response for unit testing and diagnostics."""
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
