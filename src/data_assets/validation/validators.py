"""Composable validation building blocks.

Validators can be combined and used in asset.validate() overrides.
"""

from __future__ import annotations

from typing import Callable

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
