"""Tests for RestAsset declarative config pattern."""

from __future__ import annotations

import uuid

from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.rest_asset import RestAsset
from data_assets.core.run_context import RunContext


def _ctx(**kwargs):
    return RunContext(
        run_id=uuid.uuid4(), mode=RunMode.FULL, asset_name="test", **kwargs
    )


# --- Define a test asset using RestAsset ---

class FakeTokenManager:
    def get_token(self):
        return "fake"

    def get_auth_header(self):
        return {"Authorization": "Bearer fake"}


class ItemsAsset(RestAsset):
    name = "test_items"
    source_name = "test_api"
    target_table = "test_items"
    endpoint = "/api/items"
    base_url_env = "TEST_API_URL"
    token_manager_class = FakeTokenManager
    response_path = "data.items"
    pagination = {"strategy": "offset", "page_size": 50}
    load_strategy = LoadStrategy.FULL_REPLACE

    columns = [
        Column("id", "INTEGER", nullable=False),
        Column("item_name", "TEXT"),
        Column("created_at", "TIMESTAMPTZ"),
    ]
    primary_key = ["id"]
    field_map = {"name": "item_name"}  # API returns "name", we store as "item_name"


class PageNumberAsset(RestAsset):
    name = "test_pages"
    source_name = "test_api"
    target_table = "test_pages"
    endpoint = "/api/pages"
    base_url_env = "TEST_API_URL"
    token_manager_class = FakeTokenManager
    response_path = "results"
    pagination = {"strategy": "page_number", "page_size": 10, "total_path": "meta.total"}

    columns = [
        Column("id", "TEXT", nullable=False),
        Column("value", "TEXT"),
    ]
    primary_key = ["id"]


class ListResponseAsset(RestAsset):
    """API returns a bare list (no wrapping object), like GitHub repos."""

    name = "test_list"
    source_name = "test_api"
    target_table = "test_list"
    endpoint = "/api/list"
    base_url_env = "TEST_API_URL"
    token_manager_class = FakeTokenManager
    response_path = ""  # Response IS the list
    pagination = {"strategy": "none"}

    columns = [Column("id", "INTEGER", nullable=False), Column("label", "TEXT")]
    primary_key = ["id"]


# --- Tests ---


def test_build_request_basic(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = ItemsAsset()
    spec = asset.build_request(_ctx())
    assert spec.url == "https://api.test/api/items"
    assert spec.params["limit"] == 50
    assert spec.params["offset"] == 0


def test_build_request_with_checkpoint(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = ItemsAsset()
    spec = asset.build_request(_ctx(), checkpoint={"next_offset": 100})
    assert spec.params["offset"] == 100


def test_build_request_page_number(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = PageNumberAsset()
    spec = asset.build_request(_ctx(), checkpoint={"next_page": 3})
    assert spec.params["p"] == 3
    assert spec.params["ps"] == 10


def test_parse_response_with_field_map():
    asset = ItemsAsset()
    response = {
        "data": {
            "items": [
                {"id": 1, "name": "Widget", "created_at": "2025-01-01"},
                {"id": 2, "name": "Gadget", "created_at": "2025-01-02"},
            ]
        }
    }
    df, state = asset.parse_response(response)
    assert len(df) == 2
    assert list(df.columns) == ["id", "item_name", "created_at"]
    assert df.iloc[0]["item_name"] == "Widget"  # "name" → "item_name" via field_map
    assert not state.has_more  # 2 < page_size 50


def test_parse_response_page_number_with_total():
    asset = PageNumberAsset()
    response = {
        "results": [{"id": "a", "value": "x"}, {"id": "b", "value": "y"}],
        "meta": {"total": 25},
        "paging": {"pageIndex": 1},
    }
    df, state = asset.parse_response(response)
    assert len(df) == 2
    assert state.has_more is True  # page 1 of 3 (25 total / 10 per page)
    assert state.total_records == 25
    assert state.next_page == 2


def test_parse_response_bare_list():
    asset = ListResponseAsset()
    response = [{"id": 1, "label": "A"}, {"id": 2, "label": "B"}]
    df, state = asset.parse_response(response)
    assert len(df) == 2
    assert not state.has_more  # strategy=none


def test_parse_response_empty():
    asset = ItemsAsset()
    response = {"data": {"items": []}}
    df, state = asset.parse_response(response)
    assert len(df) == 0
    assert not state.has_more


def test_pagination_config_auto_set():
    """pagination dict should be converted to PaginationConfig."""
    asset = ItemsAsset()
    assert asset.pagination_config.strategy == "offset"
    assert asset.pagination_config.page_size == 50
