"""Tests for composable validators."""

import pandas as pd

from data_assets.validation.validators import (
    compose_validators,
    validate_column_lengths,
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
