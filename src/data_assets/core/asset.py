"""Base Asset class — all assets inherit from this."""

from __future__ import annotations

from abc import ABC

import pandas as pd

from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.run_context import RunContext
from data_assets.core.types import ValidationResult


class Asset(ABC):
    """Base class for all data assets.

    Subclasses must set class-level attributes for identity and target,
    and may override transform() and validate() hooks.
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

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Post-extraction pandas transform. Override for custom logic."""
        return df

    def validate(self, df: pd.DataFrame, context: RunContext) -> ValidationResult:
        """Post-transform validation. Override for custom checks.

        Default: row count > 0 and primary key columns contain no nulls.
        """
        failures: list[str] = []

        if len(df) == 0:
            failures.append("Extracted zero rows")

        for pk_col in self.primary_key:
            if pk_col in df.columns and df[pk_col].isnull().any():
                null_count = int(df[pk_col].isnull().sum())
                failures.append(
                    f"Primary key column '{pk_col}' has {null_count} null values"
                )

        return ValidationResult(passed=len(failures) == 0, failures=failures)
