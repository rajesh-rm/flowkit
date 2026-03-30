"""APIAsset — for assets sourced from external APIs."""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

import pandas as pd

from data_assets.core.asset import Asset
from data_assets.core.enums import AssetType, LoadStrategy, ParallelMode, RunMode
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec

if TYPE_CHECKING:
    from data_assets.core.run_context import RunContext
    from data_assets.extract.token_manager import TokenManager


class APIAsset(Asset):
    """Base class for assets extracted from external HTTP APIs.

    Subclasses must implement build_request() and parse_response().
    Entity-parallel assets must also implement build_entity_request().
    """

    asset_type = AssetType.API
    default_run_mode: RunMode = RunMode.FORWARD
    load_strategy: LoadStrategy = LoadStrategy.UPSERT

    # --- Source identity ---
    source_name: str = ""
    base_url: str = ""

    # --- Token management ---
    token_manager_class: type[TokenManager] | None = None

    # --- Rate limiting & HTTP ---
    rate_limit_per_second: float = 10.0
    request_timeout: float = 60.0
    max_retries: int = 3

    # --- Pagination ---
    pagination_config: PaginationConfig = PaginationConfig(strategy="none")

    # --- Parallel extraction ---
    parallel_mode: ParallelMode = ParallelMode.NONE
    max_workers: int = 1
    total_pages_field: str | None = None
    parent_asset_name: str | None = None

    # --- Incremental support ---
    date_column: str | None = None
    api_date_param: str | None = None
    date_format: str = "%Y-%m-%dT%H:%M:%S"
    earliest_date: str | None = None

    # --- Error handling ---

    def classify_error(self, status_code: int, headers: dict) -> str:
        """Classify an HTTP error response into an action.

        Returns:
            "retry" — transient error, retry with backoff (429, 5xx)
            "skip"  — expected error, skip this request (e.g., 404 deleted entity)
            "fail"  — client error, fail immediately (4xx)

        Override per asset for source-specific behavior. For example, a
        GitHub asset might skip 404s for deleted repos during entity-parallel.
        """
        if status_code == 429 or status_code >= 500:
            return "retry"
        if status_code == 404:
            return "skip"
        return "fail"

    @abstractmethod
    def build_request(
        self, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        """Construct the HTTP request for the current extraction window."""
        ...

    @abstractmethod
    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        """Parse an API response into rows and pagination state."""
        ...

    def build_entity_request(
        self, entity_key: Any, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        """Construct request for a specific entity (ENTITY_PARALLEL mode).

        Must be overridden by entity-parallel assets. Default raises.
        """
        raise NotImplementedError(
            f"Asset '{self.name}' uses ENTITY_PARALLEL but does not implement "
            "build_entity_request()"
        )
