"""Base Asset class — all assets inherit from this."""

from __future__ import annotations

from abc import ABC

import pandas as pd
from sqlalchemy.engine import Engine

from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, RunMode, SchemaContract
from data_assets.core.run_context import RunContext
from data_assets.core.types import ValidationResult


class Asset(ABC):
    """Base class for all data assets.

    Subclasses must set class-level attributes for identity and target,
    and may override transform(), validate(), and validate_warnings() hooks.
    """

    # --- Identity ---
    name: str
    description: str = ""

    # --- Target ---
    target_schema: str = "raw"
    target_table: str = ""
    columns: list[Column] = []
    primary_key: list[str] = []

    # --- Behavior ---
    default_run_mode: RunMode = RunMode.FULL
    load_strategy: LoadStrategy = LoadStrategy.FULL_REPLACE

    # --- Schema contract ---
    schema_contract: SchemaContract = SchemaContract.EVOLVE

    def extract(
        self, engine: Engine, temp_table: str, context: RunContext,
    ) -> int | None:
        """Custom extraction logic. Override to bypass the standard API pipeline.

        Return the number of rows extracted, or None to use the default
        extraction pipeline (APIClient for APIAsset, SQL for TransformAsset).
        """
        return None

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Post-extraction pandas transform. Override for custom logic."""
        return df

    def validate(self, df: pd.DataFrame, context: RunContext) -> ValidationResult:
        """Blocking validation — must pass before promotion.

        Default: row count > 0 and primary key columns contain no nulls.
        Override to add custom blocking checks. Call super() to keep defaults.
        """
        failures: list[str] = []

        if len(df) == 0:
            failures.append("Extracted zero rows")

        for pk_col in self.primary_key:
            if pk_col not in df.columns:
                failures.append(f"Primary key column '{pk_col}' missing from data")
            elif df[pk_col].isnull().any():
                null_count = int(df[pk_col].isnull().sum())
                failures.append(
                    f"Primary key column '{pk_col}' has {null_count} null values"
                )

        return ValidationResult(passed=len(failures) == 0, failures=failures)

    def validate_warnings(self, df: pd.DataFrame, context: RunContext) -> list[str]:
        """Non-blocking warnings — logged but don't prevent promotion.

        Override to add custom warning checks (e.g., row count below expected).
        """
        return []
