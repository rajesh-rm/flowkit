"""Tests for RestAsset declarative config pattern."""

from __future__ import annotations

from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy
from data_assets.core.rest_asset import RestAsset
from tests.unit.conftest import make_ctx


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
    spec = asset.build_request(make_ctx())
    assert spec.url == "https://api.test/api/items"
    assert spec.params["limit"] == 50
    assert spec.params["offset"] == 0


def test_build_request_with_checkpoint(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = ItemsAsset()
    spec = asset.build_request(make_ctx(), checkpoint={"next_offset": 100})
    assert spec.params["offset"] == 100


def test_build_request_page_number(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = PageNumberAsset()
    spec = asset.build_request(make_ctx(), checkpoint={"next_page": 3})
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
    assert not state.has_more


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


# --- Cursor pagination ---

class CursorAsset(RestAsset):
    name = "test_cursor"
    source_name = "test_api"
    target_table = "test_cursor"
    endpoint = "/api/cursor"
    base_url_env = "TEST_API_URL"
    token_manager_class = FakeTokenManager
    response_path = "items"
    pagination = {"strategy": "cursor", "cursor_field": "next_cursor"}
    columns = [Column("id", "TEXT", nullable=False)]
    primary_key = ["id"]


def test_cursor_pagination_first_request(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    spec = CursorAsset().build_request(make_ctx())
    assert "next_cursor" not in spec.params  # no cursor on first request


def test_cursor_pagination_with_checkpoint(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    spec = CursorAsset().build_request(make_ctx(), checkpoint={"cursor": "abc123"})
    assert spec.params["next_cursor"] == "abc123"


def test_cursor_pagination_has_more():
    """has_more requires cursor present AND result_count >= page_size."""
    asset = CursorAsset()
    # page_size defaults to 100 in CursorAsset's pagination config
    items = [{"id": str(i)} for i in range(100)]
    response = {"items": items, "next_cursor": "page2"}
    _, state = asset.parse_response(response)
    assert state.has_more is True
    assert state.cursor == "page2"


def test_cursor_pagination_exhausted_no_cursor():
    asset = CursorAsset()
    response = {"items": [{"id": "1"}]}  # no cursor in response
    _, state = asset.parse_response(response)
    assert state.has_more is False


def test_cursor_pagination_exhausted_partial_page():
    """Cursor present but fewer results than page_size → last page."""
    asset = CursorAsset()
    response = {"items": [{"id": "1"}], "next_cursor": "page2"}
    _, state = asset.parse_response(response)
    assert state.has_more is False  # 1 < page_size 100
    assert state.cursor == "page2"  # cursor still captured


# --- Date filtering ---

class IncrementalAsset(RestAsset):
    name = "test_incr"
    source_name = "test_api"
    target_table = "test_incr"
    endpoint = "/api/data"
    base_url_env = "TEST_API_URL"
    token_manager_class = FakeTokenManager
    response_path = "items"
    pagination = {"strategy": "none"}
    api_date_param = "updated_since"
    columns = [Column("id", "TEXT", nullable=False)]
    primary_key = ["id"]


def test_date_param_added_when_start_date_set(monkeypatch):
    from datetime import UTC, datetime
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    ctx = make_ctx(start_date=datetime(2025, 6, 1, tzinfo=UTC))
    spec = IncrementalAsset().build_request(ctx)
    assert "updated_since" in spec.params


def test_date_param_absent_when_no_start_date(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    spec = IncrementalAsset().build_request(make_ctx())
    assert "updated_since" not in spec.params


# --- Missing fields in response ---

def test_missing_fields_default_to_none():
    """If a column is not in the API response, it should be None."""
    asset = ItemsAsset()
    response = {"data": {"items": [{"id": 1}]}}  # missing "name" and "created_at"
    df, _ = asset.parse_response(response)
    assert len(df) == 1
    assert df.iloc[0]["item_name"] is None
    assert df.iloc[0]["created_at"] is None


# --- Configurable pagination param names ---


class CustomParamAsset(RestAsset):
    """API that uses 'page'/'per_page' instead of 'p'/'ps'."""

    name = "test_custom_params"
    source_name = "test_api"
    target_table = "test_custom_params"
    endpoint = "/api/v2/items"
    base_url_env = "TEST_API_URL"
    token_manager_class = FakeTokenManager
    response_path = "data"
    pagination = {
        "strategy": "page_number",
        "page_size": 25,
        "total_path": "total_count",
        "page_size_param": "per_page",
        "page_number_param": "page",
        "page_index_path": "meta.current_page",
    }
    columns = [Column("id", "TEXT", nullable=False)]
    primary_key = ["id"]


def test_custom_param_names_in_request(monkeypatch):
    """Custom param names should appear in the request params."""
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = CustomParamAsset()
    spec = asset.build_request(make_ctx())
    assert spec.params["per_page"] == 25
    assert spec.params["page"] == 1
    assert "ps" not in spec.params
    assert "p" not in spec.params


def test_custom_param_names_with_checkpoint(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = CustomParamAsset()
    spec = asset.build_request(make_ctx(), checkpoint={"next_page": 4})
    assert spec.params["page"] == 4


def test_custom_page_index_path():
    """page_index_path should read current page from custom response path."""
    asset = CustomParamAsset()
    response = {
        "data": [{"id": "a"}],
        "total_count": 75,
        "meta": {"current_page": 2},
    }
    _, state = asset.parse_response(response)
    assert state.total_pages == 3  # ceil(75 / 25)
    assert state.next_page == 3
    assert state.has_more is True


def test_no_page_index_path_defaults_to_page_one():
    """Without page_index_path, page_number pagination assumes page 1."""
    asset = PageNumberAsset()
    response = {
        "results": [{"id": "a", "value": "x"}],
        "meta": {"total": 25},
    }
    _, state = asset.parse_response(response)
    assert state.next_page == 2  # defaults to page 1 + 1
    assert state.has_more is True


class CustomOffsetAsset(RestAsset):
    """API that uses 'count'/'skip' instead of 'limit'/'offset'."""

    name = "test_custom_offset"
    source_name = "test_api"
    target_table = "test_custom_offset"
    endpoint = "/api/records"
    base_url_env = "TEST_API_URL"
    token_manager_class = FakeTokenManager
    response_path = "items"
    pagination = {
        "strategy": "offset",
        "page_size": 20,
        "limit_param": "count",
        "offset_param": "skip",
    }
    columns = [Column("id", "TEXT", nullable=False)]
    primary_key = ["id"]


def test_custom_offset_param_names(monkeypatch):
    monkeypatch.setenv("TEST_API_URL", "https://api.test")
    asset = CustomOffsetAsset()
    spec = asset.build_request(make_ctx())
    assert spec.params["count"] == 20
    assert spec.params["skip"] == 0
    assert "limit" not in spec.params
    assert "offset" not in spec.params
