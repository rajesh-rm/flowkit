"""Tests for composable validators."""

import pandas as pd

from data_assets.validation.validators import (
    compose_validators,
    validate_no_full_null_columns,
    validate_pk_not_null,
    validate_pk_unique,
    validate_row_count,
    validate_schema_match,
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
