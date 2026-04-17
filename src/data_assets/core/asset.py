"""Base Asset class — all assets inherit from this."""

from __future__ import annotations

from abc import ABC

import pandas as pd
from sqlalchemy.engine import Engine

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode, SchemaContract
from data_assets.core.run_context import RunContext
from data_assets.core.types import ValidationResult
from data_assets.validation.validators import (
    validate_column_lengths,
    validate_column_null_rates,
    warn_oversized_strings,
)


class Asset(ABC):
    """Base class for all data assets.

    Subclasses must set class-level attributes for identity and target,
    and may override transform(), validate(), and validate_warnings() hooks.
    """

    # --- Identity ---
    name: str
    description: str = ""
    source_name: str = ""

    # --- Target ---
    target_schema: str = "raw"
    target_table: str = ""
    columns: list[Column] = []
    primary_key: list[str] = []
    indexes: list[Index] = []

    # --- Behavior ---
    default_run_mode: RunMode = RunMode.FULL
    load_strategy: LoadStrategy = LoadStrategy.FULL_REPLACE

    # --- Schema contract ---
    schema_contract: SchemaContract = SchemaContract.EVOLVE

    # --- Run resilience ---
    # A run is considered abandoned when EITHER threshold is exceeded.
    stale_heartbeat_minutes: int = 20
    max_run_hours: int = 5

    # --- Incremental support ---
    date_column: str | None = None

    # --- Data quality ---
    # Optional per-column max string lengths. When set, validate() checks
    # these limits (blocking) and validate_warnings() warns on >10k chars.
    column_max_lengths: dict[str, int] = {}

    # Maximum allowed null fraction per column (0.0–1.0). Default: 2%.
    # Columns not listed use default_null_threshold. Set a column to 1.0
    # to exempt it (e.g., EAV metric_value that is nullable by design).
    # PK columns are excluded automatically (they have a zero-null check).
    default_null_threshold: float = 0.02
    column_null_thresholds: dict[str, float] = {}

    # --- DAG generation ---
    dag_config: dict = {}

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

        Default: row count > 0, primary key columns contain no nulls,
        string columns respect column_max_lengths (if defined), and
        non-PK columns respect null rate thresholds.
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

        if self.column_max_lengths:
            length_result = validate_column_lengths(df, self.column_max_lengths)
            failures.extend(length_result.failures)

        null_rate_result = validate_column_null_rates(
            df,
            default_threshold=self.default_null_threshold,
            column_thresholds=self.column_null_thresholds,
            exclude_columns=self.primary_key,
        )
        failures.extend(null_rate_result.failures)

        return ValidationResult(passed=len(failures) == 0, failures=failures)

    def validate_warnings(self, df: pd.DataFrame, context: RunContext) -> list[str]:
        """Non-blocking warnings — logged but don't prevent promotion.

        Default: warns on any string column with values exceeding 10,000 chars.
        Override to add custom warning checks (e.g., row count below expected).
        """
        return warn_oversized_strings(df)
