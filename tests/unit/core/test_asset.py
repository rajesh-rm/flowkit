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
# Null-rate threshold validation
# ---------------------------------------------------------------------------


def test_null_rate_catches_high_null_rate():
    """A column with >2% nulls should fail validation by default."""
    asset = StubAsset()
    df = pd.DataFrame({"id": range(100), "value": [None] * 10 + ["x"] * 90})
    result = asset.validate(df, make_ctx())
    assert not result.passed
    assert any("null rate" in f for f in result.failures)


def test_null_rate_passes_low_null_rate():
    """A column with <=2% nulls should pass validation."""
    asset = StubAsset()
    df = pd.DataFrame({"id": range(100), "value": [None] * 2 + ["x"] * 98})
    result = asset.validate(df, make_ctx())
    assert result.passed


def test_null_rate_pk_excluded():
    """PK columns should not trigger null-rate failures (they have their own check)."""
    asset = StubAsset()
    df = pd.DataFrame({"id": [1, None, 3], "value": ["a", "b", "c"]})
    result = asset.validate(df, make_ctx())
    assert not result.passed
    # Only PK null failure, not a redundant null-rate failure for 'id'
    assert len(result.failures) == 1
    assert "primary key" in result.failures[0].lower()


def test_null_rate_per_column_override():
    """Subclass can raise threshold for specific columns."""

    class LooseAsset(Asset):
        name = "loose"
        target_table = "loose"
        columns = [Column("id", Integer(), nullable=False), Column("notes", Text())]
        primary_key = ["id"]
        column_null_thresholds = {"notes": 0.5}

    asset = LooseAsset()
    df = pd.DataFrame({"id": range(100), "notes": [None] * 30 + ["x"] * 70})
    result = asset.validate(df, make_ctx())
    assert result.passed


def test_null_rate_exempt_column():
    """A column set to threshold 1.0 should pass even at 100% null."""

    class EAVAsset(Asset):
        name = "eav"
        target_table = "eav"
        columns = [Column("id", Integer(), nullable=False), Column("metric_value", Text())]
        primary_key = ["id"]
        column_null_thresholds = {"metric_value": 1.0}

    asset = EAVAsset()
    df = pd.DataFrame({"id": range(10), "metric_value": [None] * 10})
    result = asset.validate(df, make_ctx())
    assert result.passed


def test_null_rate_global_threshold_override():
    """Subclass can change the global default threshold."""

    class StrictAsset(Asset):
        name = "strict"
        target_table = "strict"
        columns = [Column("id", Integer(), nullable=False), Column("value", Text())]
        primary_key = ["id"]
        default_null_threshold = 0.0

    asset = StrictAsset()
    df = pd.DataFrame({"id": range(100), "value": [None] + ["x"] * 99})
    result = asset.validate(df, make_ctx())
    assert not result.passed


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
