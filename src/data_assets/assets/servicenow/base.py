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
from sqlalchemy import Boolean as SABool
from sqlalchemy import DateTime as SADateTime
from sqlalchemy import Float as SAFloat
from sqlalchemy.engine import Engine

from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import ServiceNowTokenManager
from data_assets.load.loader import write_to_temp

logger = logging.getLogger(__name__)


class ServiceNowTableAsset(APIAsset):
    """Base for ServiceNow assets using pysnc GlideRecord.

    Subclasses must set: name, target_table, table_name, columns.
    """

    source_name = "servicenow"
    target_schema = "raw"
    token_manager_class = ServiceNowTokenManager

    pagination_config = PaginationConfig(strategy="keyset", page_size=1000)
    parallel_mode = ParallelMode.NONE
    max_workers = 1

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD

    primary_key = ["sys_id"]
    date_column = "sys_updated_on"
    indexes = [
        Index(columns=("sys_updated_on",)),
    ]

    # Common ServiceNow column limits — subclasses inherit; override to extend.
    column_max_lengths = {
        "sys_id": 32,           # GUID hex without dashes
        "number": 40,           # INCxxxxxxx, CHGxxxxxxx, etc.
        "state": 100,
        "priority": 100,
        "category": 200,
        "assigned_to": 32,      # sys_id reference
        "assignment_group": 32,  # sys_id reference
    }

    # Subclass must set this to the ServiceNow table name (e.g., "incident")
    table_name: str = ""

    # -----------------------------------------------------------------------
    # pysnc-based extraction (bypasses APIClient/httpx pipeline)
    # -----------------------------------------------------------------------

    def _create_pysnc_client(self):
        """Create an authenticated pysnc ServiceNowClient via token manager."""
        from pysnc import ServiceNowClient

        token_mgr = self.token_manager_class()
        return ServiceNowClient(token_mgr.instance, token_mgr.get_pysnc_auth())

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

        max_p = context.params.get("max_pages")
        total_rows = 0
        batch: list[dict] = []
        pages_fetched = 0
        start_time = time.monotonic()
        last_log_time = start_time

        for record in gr:
            batch.append(record.serialize())

            if len(batch) >= batch_size:
                df = self._batch_to_df(batch)
                total_rows += write_to_temp(engine, temp_table, df)
                batch = []
                pages_fetched += 1

                if max_p is not None and pages_fetched >= max_p:
                    logger.info(
                        "max_pages=%d reached for %s — stopping early (developer override)",
                        max_p, self.name,
                    )
                    break

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

    def _validate_and_select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Ensure all declared columns exist in *df*, then select only those columns."""
        column_names = [c.name for c in self.columns]
        missing = [c for c in column_names if c not in df.columns]
        if missing:
            raise ValueError(
                f"{self.name}: declared columns missing from API response: {missing}. "
                f"Either the source API no longer returns these fields, or the "
                f"column list in the asset definition needs updating."
            )
        return df[column_names]

    def _batch_to_df(self, batch: list[dict]) -> pd.DataFrame:
        """Convert a batch of serialized records to a DataFrame with declared columns."""
        df = self._validate_and_select_columns(pd.DataFrame(batch))

        # Convert string "true"/"false" from pysnc to native Python booleans
        for col in self.columns:
            if isinstance(col.sa_type, SABool):
                df[col.name] = df[col.name].map(
                    {"true": True, "false": False}
                ).astype("boolean")
            elif isinstance(col.sa_type, SAFloat):
                df[col.name] = pd.to_numeric(df[col.name], errors="coerce")
            elif isinstance(col.sa_type, SADateTime):
                df[col.name] = df[col.name].replace("", None)
                df[col.name] = pd.to_datetime(df[col.name], utc=True, errors="coerce")

        return df

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
            df = self._validate_and_select_columns(pd.DataFrame(results))
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
