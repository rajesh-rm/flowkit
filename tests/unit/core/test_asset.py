"""Tests for the base Asset class: default transform and validation."""

from __future__ import annotations

import pandas as pd

from data_assets.core.asset import Asset
from data_assets.core.column import Column
from tests.unit.conftest import make_ctx


class StubAsset(Asset):
    name = "stub"
    target_table = "stub"
    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("value", "TEXT"),
    ]
    primary_key = ["id"]


def test_default_transform_is_identity():
    asset = StubAsset()
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    result = asset.transform(df)
    pd.testing.assert_frame_equal(result, df)


def test_default_validate_passes():
    asset = StubAsset()
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    result = asset.validate(df, make_ctx())
    assert result.passed


def test_default_validate_fails_empty():
    asset = StubAsset()
    df = pd.DataFrame({"id": [], "value": []})
    result = asset.validate(df, make_ctx())
    assert not result.passed
    assert "zero rows" in result.failures[0].lower()


def test_default_validate_fails_null_pk():
    asset = StubAsset()
    df = pd.DataFrame({"id": [1, None], "value": ["a", "b"]})
    result = asset.validate(df, make_ctx())
    assert not result.passed
    assert "null" in result.failures[0].lower()


def test_default_validate_fails_missing_pk_column():
    """PK column absent from DataFrame should be caught, not silently pass."""
    asset = StubAsset()
    df = pd.DataFrame({"value": ["a", "b"]})  # 'id' column missing
    result = asset.validate(df, make_ctx())
    assert not result.passed
    assert "missing" in result.failures[0].lower()
