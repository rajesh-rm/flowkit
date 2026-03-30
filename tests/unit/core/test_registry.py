"""Tests for asset registry: registration, lookup, discovery."""

from __future__ import annotations

import pytest

from data_assets.core.asset import Asset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import _registry, all_assets, get, register


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear the registry before and after each test."""
    saved = dict(_registry)
    _registry.clear()
    yield
    _registry.clear()
    _registry.update(saved)


def _make_asset(name: str) -> type[Asset]:
    """Create a minimal concrete asset class."""
    cls = type(
        name,
        (Asset,),
        {
            "name": name,
            "target_table": name,
            "columns": [Column("id", "INTEGER", nullable=False)],
            "primary_key": ["id"],
            "load_strategy": LoadStrategy.FULL_REPLACE,
            "default_run_mode": RunMode.FULL,
        },
    )
    return cls


def test_register_and_get():
    cls = _make_asset("test_asset")
    register(cls)
    assert get("test_asset") is cls


def test_get_unknown_raises():
    with pytest.raises(KeyError, match="not_registered"):
        get("not_registered")


def test_all_assets():
    cls1 = _make_asset("asset_a")
    cls2 = _make_asset("asset_b")
    register(cls1)
    register(cls2)
    assets = all_assets()
    assert set(assets.keys()) == {"asset_a", "asset_b"}


def test_register_overwrites_duplicate():
    cls1 = _make_asset("dup")
    cls2 = _make_asset("dup")
    register(cls1)
    register(cls2)
    assert get("dup") is cls2


def test_register_as_decorator():
    @register
    class MyAsset(Asset):
        name = "decorated"
        target_table = "decorated"
        columns = [Column("id", "INTEGER", nullable=False)]
        primary_key = ["id"]

    assert get("decorated") is MyAsset
