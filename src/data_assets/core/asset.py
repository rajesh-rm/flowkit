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
    warn_column_null_rates,
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

    # Columns that MAY be absent from individual API response dicts.
    # Listed columns are exempted from the missing-key check (which fails
    # the run when a required key is absent from the raw response).
    # Distinct from Column(nullable=True): that controls DB nullability;
    # this controls API-response shape.
    # PK columns and columns used in any index cannot be listed here
    # (enforced at registry validation time, so the error surfaces at import).
    optional_columns: list[str] = []

    # --- Behavior ---
    default_run_mode: RunMode = RunMode.FULL
    load_strategy: LoadStrategy = LoadStrategy.FULL_REPLACE

    # --- Schema contract ---
    schema_contract: SchemaContract = SchemaContract.EVOLVE

    # --- Sensitive data tokenization ---
    # Every concrete asset MUST set this explicitly (True or False). The
    # sentinel `None` is rejected at registration time so the choice can
    # never be made by accident. When True, at least one Column must have
    # `sensitive=True`; the registry verifies this and ensures sensitive
    # columns are not referenced by any explicit Index or Index.include
    # (sensitive columns ARE permitted in primary_key).
    contains_sensitive_data: bool | None = None

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

    # Null-rate WARNING threshold per column (0.0–1.0). Default: 2%.
    # Non-blocking: when exceeded, Asset.validate_warnings() emits a single
    # consolidated warning listing all offending columns. Columns not listed
    # use default_null_threshold. Set a column to 1.0 to silence warnings
    # (e.g., EAV metric_value that is nullable by design). PK columns are
    # excluded automatically. For hard-failing on absent API keys, use
    # optional_columns instead — null rate and missing-key are different signals.
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

    def sensitive_column_names(self) -> list[str]:
        """Return names of columns marked sensitive=True, or empty list.

        Empty when the asset declares ``contains_sensitive_data=False``
        (no auditing of column flags) or when ``contains_sensitive_data``
        is True but no column carries ``sensitive=True`` (which the
        registry rejects, but defensively still returns []).
        """
        if not self.contains_sensitive_data:
            return []
        return [c.name for c in self.columns if getattr(c, "sensitive", False)]

    def validate(self, df: pd.DataFrame, context: RunContext) -> ValidationResult:
        """Blocking validation — must pass before promotion.

        Default: row count > 0, primary key columns contain no nulls, and
        string columns respect column_max_lengths (if defined).
        Missing API-response keys are caught earlier, in parse_response, via
        the fail-fast MissingKeyError path — they never reach this method.
        Null rate is handled as a warning in validate_warnings(), not here.
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

        return ValidationResult(passed=len(failures) == 0, failures=failures)

    def validate_warnings(self, df: pd.DataFrame, context: RunContext) -> list[str]:
        """Non-blocking warnings — logged but don't prevent promotion.

        Defaults: one warning for any string column exceeding 10,000 chars, and
        one consolidated warning listing every column whose null rate exceeds
        its threshold. Override to add custom warning checks.
        """
        warnings = warn_oversized_strings(df)
        warnings.extend(
            warn_column_null_rates(
                df,
                default_threshold=self.default_null_threshold,
                column_thresholds=self.column_null_thresholds,
                exclude_columns=self.primary_key,
            )
        )
        return warnings
