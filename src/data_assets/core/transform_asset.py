"""TransformAsset — for assets derived from existing Postgres tables."""

from __future__ import annotations

from abc import abstractmethod

from data_assets.core.asset import Asset
from data_assets.core.enums import AssetType, LoadStrategy, RunMode
from data_assets.core.run_context import RunContext
from data_assets.db.dialect import Dialect


class TransformAsset(Asset):
    """Base class for assets produced by SQL transforms on existing tables.

    Subclasses must implement query() to return a SELECT statement.
    """

    asset_type = AssetType.TRANSFORM
    default_run_mode: RunMode = RunMode.TRANSFORM
    load_strategy: LoadStrategy = LoadStrategy.FULL_REPLACE
    target_schema: str = "mart"

    # Transforms read from already-tokenized source tables and write derived
    # rows. Tokenization is intentionally not re-applied at the transform
    # layer (see plan: Non-goals). Subclasses that introduce a NEW sensitive
    # column derived from non-sensitive sources can override.
    contains_sensitive_data = False

    # --- Source ---
    source_tables: list[str] = []

    query_timeout_seconds: int = 300

    @abstractmethod
    def query(self, context: RunContext, dialect: Dialect) -> str:
        """Return a SQL SELECT producing the output rows.

        The result set columns must match this asset's `columns` definition.
        Use ``dialect`` for any SQL that differs between Postgres and
        MariaDB (week truncation, date arithmetic, integer casts, etc.).
        """
        ...
