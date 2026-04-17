"""Tests for the base Asset class: default transform and validation."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import Integer, Text

from data_assets.core.asset import Asset
from data_assets.core.column import Column
from tests.unit.conftest import make_ctx


class StubAsset(Asset):
    name = "stub"
    target_table = "stub"
    columns = [
        Column("id", Integer(), nullable=False),
        Column("value", Text()),
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


# ---------------------------------------------------------------------------
# APIAsset: classify_error and should_stop
# ---------------------------------------------------------------------------

from data_assets.core.api_asset import APIAsset
from data_assets.core.types import PaginationState
from sqlalchemy import Integer, Text


class ConcreteAPIAsset(APIAsset):
    """Minimal concrete subclass for testing APIAsset default methods."""

    name = "test_api"
    target_table = "test_api"
    columns = [Column("id", Integer(), nullable=False)]
    primary_key = ["id"]

    def parse_response(self, response):
        return pd.DataFrame(), PaginationState(has_more=False)


class TestClassifyError:
    def test_429_returns_retry(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(429, {}) == "retry"

    def test_500_returns_retry(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(500, {}) == "retry"

    def test_502_returns_retry(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(502, {}) == "retry"

    def test_503_returns_retry(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(503, {}) == "retry"

    def test_404_returns_skip(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(404, {}) == "skip"

    def test_400_returns_fail(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(400, {}) == "fail"

    def test_401_returns_fail(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(401, {}) == "fail"

    def test_403_returns_fail(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(403, {}) == "fail"

    def test_422_returns_fail(self):
        asset = ConcreteAPIAsset()
        assert asset.classify_error(422, {}) == "fail"


class TestShouldStop:
    def test_default_returns_false(self):
        asset = ConcreteAPIAsset()
        df = pd.DataFrame({"id": [1, 2, 3]})
        result = asset.should_stop(df, make_ctx())
        assert result is False

    def test_default_returns_false_with_empty_df(self):
        asset = ConcreteAPIAsset()
        df = pd.DataFrame()
        result = asset.should_stop(df, make_ctx())
        assert result is False


class TestBuildEntityRequest:
    def test_default_raises_not_implemented(self):
        asset = ConcreteAPIAsset()
        import pytest as _pytest
        with _pytest.raises(NotImplementedError, match="ENTITY_PARALLEL"):
            asset.build_entity_request("key1", make_ctx())

    def test_build_request_default_raises_not_implemented(self):
        asset = ConcreteAPIAsset()
        import pytest as _pytest
        with _pytest.raises(NotImplementedError, match="build_request"):
            asset.build_request(make_ctx())
