"""RestAsset — declarative base class for standard REST API assets.

For the 80% of assets that follow a standard pattern (fetch JSON from a
REST endpoint, paginate, map fields to columns), RestAsset eliminates
the need to write build_request() and parse_response() manually.

Usage:
    @register
    class MyAsset(RestAsset):
        name = "my_asset"
        target_table = "my_asset"
        endpoint = "/api/items"
        base_url_env = "MY_API_URL"
        token_manager_class = MyTokenManager
        response_path = "items"           # JSON path to records list
        pagination = {"strategy": "offset", "page_size": 100}
        columns = [Column("id", "TEXT", nullable=False), ...]
        primary_key = ["id"]
        field_map = {"api_field": "column_name"}  # Optional renames

For complex APIs that need custom request/response logic, subclass
APIAsset directly instead.
"""

from __future__ import annotations

import math
import os
from typing import Any

import pandas as pd

from data_assets.core.api_asset import APIAsset
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.flatten import _get_nested


class RestAsset(APIAsset):
    """Declarative REST API asset — no build_request/parse_response needed.

    Class attributes (set on your subclass):
        endpoint:       API path (e.g., "/api/projects/search")
        base_url_env:   Env var name for the base URL (e.g., "SONARQUBE_URL")
        response_path:  Dot-path to the records list in the response JSON.
                        Use "" or None if the response IS the list (like GitHub).
        pagination:     Dict with keys: strategy, page_size, total_path (optional).
                        Shorthand for PaginationConfig. Or set pagination_config directly.
        field_map:      Dict mapping API field names → column names.
                        Only needed for fields that need renaming.
                        Fields with matching names are mapped automatically.
    """

    # --- Declarative config (set on subclass) ---
    endpoint: str = ""
    base_url_env: str = ""
    response_path: str = ""
    pagination: dict | None = None
    field_map: dict[str, str] = {}

    _reverse_field_map: dict[str, str] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Convert the pagination dict shorthand to PaginationConfig."""
        super().__init_subclass__(**kwargs)
        if "pagination" in cls.__dict__ and cls.pagination is not None:
            p = cls.pagination
            cls.pagination_config = PaginationConfig(
                strategy=p.get("strategy", "none"),
                page_size=p.get("page_size", 100),
                cursor_field=p.get("cursor_field"),
                total_path=p.get("total_path"),
                page_size_param=p.get("page_size_param", "ps"),
                page_number_param=p.get("page_number_param", "p"),
                limit_param=p.get("limit_param", "limit"),
                offset_param=p.get("offset_param", "offset"),
                page_index_path=p.get("page_index_path"),
            )
        if cls.field_map:
            cls._reverse_field_map = {v: k for k, v in cls.field_map.items()}

    def build_request(
        self, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        base = os.environ.get(self.base_url_env, self.base_url)
        url = f"{base}{self.endpoint}"

        params: dict[str, Any] = {}
        strategy = self.pagination_config.strategy
        page_size = self.pagination_config.page_size

        if strategy == "page_number":
            params[self.pagination_config.page_size_param] = page_size
            params[self.pagination_config.page_number_param] = (
                checkpoint.get("next_page", 1) if checkpoint else 1
            )
        elif strategy == "offset":
            params[self.pagination_config.limit_param] = page_size
            params[self.pagination_config.offset_param] = (
                (checkpoint.get("next_offset") or 0) if checkpoint else 0
            )
        elif strategy == "cursor":
            if checkpoint and checkpoint.get("cursor"):
                params[self.pagination_config.cursor_field or "cursor"] = checkpoint["cursor"]

        # Add date filter if incremental and context has start_date
        if context.start_date and self.api_date_param:
            params[self.api_date_param] = context.start_date.isoformat()

        return RequestSpec(method="GET", url=url, params=params)

    def parse_response(
        self, response: Any
    ) -> tuple[pd.DataFrame, PaginationState]:
        # Extract records from response
        if self.response_path:
            records_raw = _get_nested(response, self.response_path) or []
        else:
            # Response IS the list (e.g., GitHub repos returns a list directly)
            records_raw = response if isinstance(response, list) else []

        # Map fields: apply field_map renames, keep columns that match by name
        column_names = {c.name for c in self.columns}
        reverse_map = self._reverse_field_map

        records = []
        for raw in records_raw:
            row: dict[str, Any] = {}
            for col_name in column_names:
                api_field = reverse_map.get(col_name, col_name)
                row[col_name] = _get_nested(raw, api_field)
            records.append(row)

        df = pd.DataFrame(records, columns=[c.name for c in self.columns])

        # Compute pagination state
        state = self._parse_pagination(response, len(records_raw))
        return df, state

    def _parse_pagination(
        self, response: Any, result_count: int
    ) -> PaginationState:
        strategy = self.pagination_config.strategy
        page_size = self.pagination_config.page_size

        if strategy == "page_number":
            total_path = self.pagination_config.total_path
            total = _get_nested(response, total_path) if total_path else None
            if total is not None:
                total_pages = math.ceil(int(total) / page_size)
                # Read current page index from response if path is configured
                page_index_path = self.pagination_config.page_index_path
                if page_index_path:
                    raw = _get_nested(response, page_index_path)
                    page_index = raw if raw is not None else 1
                else:
                    page_index = 1
                return PaginationState(
                    has_more=page_index < total_pages,
                    next_page=page_index + 1,
                    total_pages=total_pages,
                    total_records=int(total),
                )
            # No total — use result count heuristic
            return PaginationState(has_more=result_count >= page_size)

        if strategy == "offset":
            return PaginationState(
                has_more=result_count >= page_size,
                next_offset=None,  # Tracked by sequential extractor via checkpoint
            )

        if strategy == "cursor":
            cursor_field = self.pagination_config.cursor_field or "cursor"
            cursor = _get_nested(response, cursor_field) if isinstance(response, dict) else None
            return PaginationState(
                has_more=cursor is not None and result_count >= page_size,
                cursor=cursor,
            )

        if strategy == "none":
            return PaginationState(has_more=False)

        raise ValueError(
            f"Unknown pagination strategy '{strategy}'. "
            "Expected: page_number, offset, cursor, or none."
        )
