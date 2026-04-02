"""TransformAsset — for assets derived from existing Postgres tables."""

from __future__ import annotations

from abc import abstractmethod

from data_assets.core.asset import Asset
from data_assets.core.enums import AssetType, LoadStrategy, RunMode
from data_assets.core.run_context import RunContext


class TransformAsset(Asset):
    """Base class for assets produced by SQL transforms on existing tables.

    Subclasses must implement query() to return a SELECT statement.
    """

    asset_type = AssetType.TRANSFORM
    default_run_mode: RunMode = RunMode.TRANSFORM
    load_strategy: LoadStrategy = LoadStrategy.FULL_REPLACE
    target_schema: str = "mart"

    # --- Source ---
    source_tables: list[str] = []

    @abstractmethod
    def query(self, context: RunContext) -> str:
        """Return a SQL SELECT producing the output rows.

        The result set columns must match this asset's `columns` definition.
        """
        ...
