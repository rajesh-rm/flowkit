"""Tests for composable validators."""

import pandas as pd

from data_assets.validation.validators import (
    compose_validators,
    validate_column_lengths,
    validate_column_null_rates,
    validate_no_full_null_columns,
    validate_pk_not_null,
    validate_pk_unique,
    validate_row_count,
    validate_schema_match,
    warn_oversized_strings,
)


def test_row_count_pass():
    df = pd.DataFrame({"a": [1, 2, 3]})
    result = validate_row_count(df, min_rows=1)
    assert result.passed


def test_row_count_fail():
    df = pd.DataFrame({"a": []})
    result = validate_row_count(df, min_rows=1)
    assert not result.passed


def test_pk_not_null_pass():
    df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"]})
    result = validate_pk_not_null(df, primary_key=["id"])
    assert result.passed


def test_pk_not_null_fail():
    df = pd.DataFrame({"id": [1, None], "name": ["a", "b"]})
    result = validate_pk_not_null(df, primary_key=["id"])
    assert not result.passed
    assert "null" in result.failures[0].lower()


def test_pk_unique_pass():
    df = pd.DataFrame({"id": [1, 2, 3]})
    result = validate_pk_unique(df, primary_key=["id"])
    assert result.passed


def test_pk_unique_fail():
    df = pd.DataFrame({"id": [1, 1, 2]})
    result = validate_pk_unique(df, primary_key=["id"])
    assert not result.passed


def test_no_full_null_columns_pass():
    df = pd.DataFrame({"a": [1, 2], "b": [None, "x"]})
    result = validate_no_full_null_columns(df)
    assert result.passed


def test_no_full_null_columns_fail():
    df = pd.DataFrame({"a": [1, 2], "b": [None, None]})
    result = validate_no_full_null_columns(df)
    assert not result.passed


def test_schema_match_pass():
    df = pd.DataFrame({"id": [1], "name": ["a"]})
    result = validate_schema_match(df, expected_columns=["id", "name"])
    assert result.passed


def test_schema_match_fail():
    df = pd.DataFrame({"id": [1]})
    result = validate_schema_match(df, expected_columns=["id", "name"])
    assert not result.passed


def test_compose_validators():
    df = pd.DataFrame({"id": [1, None]})
    combined = compose_validators(
        lambda d: validate_row_count(d, min_rows=1),
        lambda d: validate_pk_not_null(d, primary_key=["id"]),
    )
    result = combined(df)
    assert not result.passed
    assert len(result.failures) == 1  # row count passes, PK null fails


# ---------------------------------------------------------------------------
# validate_column_lengths
# ---------------------------------------------------------------------------


def test_column_lengths_pass():
    df = pd.DataFrame({"name": ["alice", "bob"], "id": ["abc", "def"]})
    result = validate_column_lengths(df, {"name": 100, "id": 10})
    assert result.passed


def test_column_lengths_at_limit():
    df = pd.DataFrame({"code": ["A" * 40]})
    result = validate_column_lengths(df, {"code": 40})
    assert result.passed


def test_column_lengths_fail():
    df = pd.DataFrame({"name": ["a" * 50, "b" * 200]})
    result = validate_column_lengths(df, {"name": 100})
    assert not result.passed
    assert "1 value(s)" in result.failures[0]
    assert "longest: 200" in result.failures[0]


def test_column_lengths_multiple_violations():
    df = pd.DataFrame({"name": ["x" * 150, "y" * 120]})
    result = validate_column_lengths(df, {"name": 100})
    assert not result.passed
    assert "2 value(s)" in result.failures[0]


def test_column_lengths_missing_column_ignored():
    df = pd.DataFrame({"name": ["alice"]})
    result = validate_column_lengths(df, {"missing_col": 10})
    assert result.passed


def test_column_lengths_null_values_ignored():
    df = pd.DataFrame({"name": [None, "alice", None]})
    result = validate_column_lengths(df, {"name": 10})
    assert result.passed


def test_column_lengths_empty_df():
    df = pd.DataFrame({"name": pd.Series([], dtype="object")})
    result = validate_column_lengths(df, {"name": 10})
    assert result.passed


# ---------------------------------------------------------------------------
# warn_oversized_strings
# ---------------------------------------------------------------------------


def test_warn_oversized_no_warnings():
    df = pd.DataFrame({"name": ["short"], "count": [42]})
    warnings = warn_oversized_strings(df, threshold=100)
    assert warnings == []


def test_warn_oversized_detects_large_value():
    df = pd.DataFrame({"description": ["x" * 15_000]})
    warnings = warn_oversized_strings(df, threshold=10_000)
    assert len(warnings) == 1
    assert "description" in warnings[0]
    assert "15000" in warnings[0]


def test_warn_oversized_skips_non_string_columns():
    df = pd.DataFrame({"count": [99999999999999]})
    warnings = warn_oversized_strings(df, threshold=5)
    assert warnings == []


# ---------------------------------------------------------------------------
# validate_column_null_rates
# ---------------------------------------------------------------------------


def test_null_rates_pass_under_threshold():
    """Columns with null rates below default 2% should pass."""
    df = pd.DataFrame({"a": [1] * 99 + [None], "b": ["x"] * 100})
    result = validate_column_null_rates(df, default_threshold=0.02)
    assert result.passed


def test_null_rates_fail_over_threshold():
    """Column exceeding 2% null rate should fail."""
    df = pd.DataFrame({"a": [1] * 90 + [None] * 10})  # 10% null
    result = validate_column_null_rates(df, default_threshold=0.02)
    assert not result.passed
    assert "10.0%" in result.failures[0]
    assert "exceeds threshold 2.0%" in result.failures[0]


def test_null_rates_per_column_override():
    """Per-column threshold should override the default."""
    df = pd.DataFrame({"a": [1] * 80 + [None] * 20})  # 20% null
    result = validate_column_null_rates(
        df, default_threshold=0.02, column_thresholds={"a": 0.25}
    )
    assert result.passed


def test_null_rates_exempt_column():
    """Column with threshold 1.0 should be exempt from checks."""
    df = pd.DataFrame({"a": [None] * 100})  # 100% null
    result = validate_column_null_rates(
        df, default_threshold=0.02, column_thresholds={"a": 1.0}
    )
    assert result.passed


def test_null_rates_exclude_columns():
    """Excluded columns (e.g., PKs) should not be checked."""
    df = pd.DataFrame({"pk": [1, None, None], "val": ["x", "y", "z"]})
    result = validate_column_null_rates(
        df, default_threshold=0.02, exclude_columns=["pk"]
    )
    assert result.passed


def test_null_rates_empty_df():
    """Empty DataFrame should pass (zero-row check is separate)."""
    df = pd.DataFrame({"a": pd.Series([], dtype="object")})
    result = validate_column_null_rates(df, default_threshold=0.02)
    assert result.passed


def test_null_rates_at_threshold_boundary():
    """Null rate exactly at threshold should pass (> not >=)."""
    df = pd.DataFrame({"a": [1] * 98 + [None] * 2})  # 2/100 = 0.02
    result = validate_column_null_rates(df, default_threshold=0.02)
    assert result.passed


def test_null_rates_multiple_failures():
    """Multiple columns can fail independently."""
    df = pd.DataFrame({
        "a": [None] * 50 + [1] * 50,
        "b": [None] * 30 + ["x"] * 70,
        "c": ["ok"] * 100,
    })
    result = validate_column_null_rates(df, default_threshold=0.02)
    assert not result.passed
    assert len(result.failures) == 2


def test_null_rates_zero_null_rate():
    """Column with no nulls should always pass."""
    df = pd.DataFrame({"a": [1, 2, 3]})
    result = validate_column_null_rates(df, default_threshold=0.0)
    assert result.passed


def test_null_rates_failure_message_format():
    """Failure message should include column name, rate, count, and threshold."""
    df = pd.DataFrame({"score": [None] * 5 + [1] * 5})
    result = validate_column_null_rates(df, default_threshold=0.02)
    assert not result.passed
    msg = result.failures[0]
    assert "score" in msg
    assert "50.0%" in msg
    assert "5/10" in msg
    assert "2.0%" in msg
