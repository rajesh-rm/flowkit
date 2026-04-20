"""Composable validation building blocks.

Validators can be combined and used in asset.validate() overrides.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from data_assets.core.types import ValidationResult


def validate_row_count(df: pd.DataFrame, min_rows: int = 1) -> ValidationResult:
    """Check that the DataFrame has at least min_rows rows."""
    if len(df) < min_rows:
        return ValidationResult(
            passed=False,
            failures=[f"Expected at least {min_rows} rows, got {len(df)}"],
        )
    return ValidationResult(passed=True)


def validate_pk_not_null(
    df: pd.DataFrame, primary_key: list[str]
) -> ValidationResult:
    """Check that primary key columns contain no nulls."""
    failures: list[str] = []
    for col in primary_key:
        if col in df.columns and df[col].isnull().any():
            nulls = int(df[col].isnull().sum())
            failures.append(f"PK column '{col}' has {nulls} null values")
    return ValidationResult(passed=len(failures) == 0, failures=failures)


def validate_pk_unique(
    df: pd.DataFrame, primary_key: list[str]
) -> ValidationResult:
    """Check that primary key values are unique."""
    if not primary_key or not all(c in df.columns for c in primary_key):
        return ValidationResult(passed=True)

    duplicates = df.duplicated(subset=primary_key, keep=False).sum()
    if duplicates > 0:
        return ValidationResult(
            passed=False,
            failures=[f"Found {duplicates} duplicate PK rows"],
        )
    return ValidationResult(passed=True)


def validate_no_full_null_columns(df: pd.DataFrame) -> ValidationResult:
    """Check that no column is entirely null."""
    failures: list[str] = []
    for col in df.columns:
        if df[col].isnull().all():
            failures.append(f"Column '{col}' is entirely null")
    return ValidationResult(passed=len(failures) == 0, failures=failures)


def validate_column_null_rates(
    df: pd.DataFrame,
    default_threshold: float = 0.02,
    column_thresholds: dict[str, float] | None = None,
    exclude_columns: list[str] | None = None,
) -> ValidationResult:
    """Check that no column exceeds its allowed null rate.

    Args:
        df: DataFrame to validate.
        default_threshold: Maximum null fraction (0.0–1.0) for columns
            not in column_thresholds. Default 0.02 (2%).
        column_thresholds: Per-column overrides. Map column name to max
            allowed null fraction. Use 1.0 to exempt a column entirely.
        exclude_columns: Columns to skip (e.g., primary key columns,
            which have their own null check).
    """
    if len(df) == 0:
        return ValidationResult(passed=True)

    column_thresholds = column_thresholds or {}
    exclude = set(exclude_columns or [])
    failures: list[str] = []
    total_rows = len(df)

    for col in df.columns:
        if col in exclude:
            continue
        threshold = column_thresholds.get(col, default_threshold)
        if threshold >= 1.0:
            continue
        null_count = int(df[col].isnull().sum())
        null_rate = null_count / total_rows
        if null_rate > threshold:
            failures.append(
                f"Column '{col}' has {null_rate:.1%} null rate "
                f"({null_count}/{total_rows} rows), "
                f"exceeds threshold {threshold:.1%}"
            )

    return ValidationResult(passed=len(failures) == 0, failures=failures)


def warn_column_null_rates(
    df: pd.DataFrame,
    default_threshold: float = 0.02,
    column_thresholds: dict[str, float] | None = None,
    exclude_columns: list[str] | None = None,
) -> list[str]:
    """Return a single consolidated warning when columns exceed their null rate.

    Non-blocking wrapper over validate_column_null_rates() intended for
    validate_warnings(). Emits zero or one string — per-column offenders are
    joined so a single asset run produces one null-rate warning, not many.
    """
    result = validate_column_null_rates(
        df,
        default_threshold=default_threshold,
        column_thresholds=column_thresholds,
        exclude_columns=exclude_columns,
    )
    if result.passed:
        return []
    return ["High null rate: " + "; ".join(result.failures)]


def validate_schema_match(
    df: pd.DataFrame, expected_columns: list[str]
) -> ValidationResult:
    """Check that all expected columns are present in the DataFrame."""
    missing = set(expected_columns) - set(df.columns)
    if missing:
        return ValidationResult(
            passed=False,
            failures=[f"Missing columns: {sorted(missing)}"],
        )
    return ValidationResult(passed=True)


def _str_lengths(series: pd.Series) -> pd.Series | None:
    """Compute string lengths for non-null values, or None if empty."""
    str_col = series.dropna().astype(str)
    if str_col.empty:
        return None
    return str_col.str.len()


def validate_column_lengths(
    df: pd.DataFrame,
    max_lengths: dict[str, int],
) -> ValidationResult:
    """Check that string columns don't exceed specified max lengths.

    Args:
        df: DataFrame to validate.
        max_lengths: Mapping of column name to maximum allowed character length.
    """
    failures: list[str] = []
    for col, limit in max_lengths.items():
        if col not in df.columns:
            continue
        lengths = _str_lengths(df[col])
        if lengths is None:
            continue
        longest = int(lengths.max())
        if longest > limit:
            violations = int((lengths > limit).sum())
            failures.append(
                f"Column '{col}' has {violations} value(s) exceeding "
                f"max length {limit} (longest: {longest} chars)"
            )
    return ValidationResult(passed=len(failures) == 0, failures=failures)


def warn_oversized_strings(
    df: pd.DataFrame,
    threshold: int = 10_000,
) -> list[str]:
    """Return warnings for string columns with values exceeding a threshold.

    Non-blocking — meant for validate_warnings(), not validate().
    """
    warnings: list[str] = []
    for col in df.columns:
        if not pd.api.types.is_string_dtype(df[col]):
            continue
        lengths = _str_lengths(df[col])
        if lengths is None:
            continue
        longest = int(lengths.max())
        if longest > threshold:
            warnings.append(
                f"Column '{col}' has value(s) exceeding {threshold} chars "
                f"(longest: {longest})"
            )
    return warnings


def compose_validators(
    *validators: Callable[[pd.DataFrame], ValidationResult],
) -> Callable[[pd.DataFrame], ValidationResult]:
    """Combine multiple validators into a single function."""

    def combined(df: pd.DataFrame) -> ValidationResult:
        all_failures: list[str] = []
        for v in validators:
            result = v(df)
            all_failures.extend(result.failures)
        return ValidationResult(
            passed=len(all_failures) == 0, failures=all_failures
        )

    return combined
