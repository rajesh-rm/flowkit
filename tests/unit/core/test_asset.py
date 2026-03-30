"""Tests for the base Asset class: default transform and validation."""

from __future__ import annotations

import uuid

import pandas as pd

from data_assets.core.asset import Asset
from data_assets.core.column import Column
from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext


class StubAsset(Asset):
    name = "stub"
    target_table = "stub"
    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("value", "TEXT"),
    ]
    primary_key = ["id"]


def _ctx():
    return RunContext(
        run_id=uuid.uuid4(), mode=RunMode.FULL, asset_name="stub"
    )


def test_default_transform_is_identity():
    asset = StubAsset()
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    result = asset.transform(df)
    pd.testing.assert_frame_equal(result, df)


def test_default_validate_passes():
    asset = StubAsset()
    df = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    result = asset.validate(df, _ctx())
    assert result.passed


def test_default_validate_fails_empty():
    asset = StubAsset()
    df = pd.DataFrame({"id": [], "value": []})
    result = asset.validate(df, _ctx())
    assert not result.passed
    assert "zero rows" in result.failures[0].lower()


def test_default_validate_fails_null_pk():
    asset = StubAsset()
    df = pd.DataFrame({"id": [1, None], "value": ["a", "b"]})
    result = asset.validate(df, _ctx())
    assert not result.passed
    assert "null" in result.failures[0].lower()
