"""Tests for pagination helper functions."""

from data_assets.core.types import PaginationConfig, PaginationState
from data_assets.extract.pagination import next_request_params


def test_next_params_cursor():
    cfg = PaginationConfig(strategy="cursor", cursor_field="after")
    state = PaginationState(has_more=True, cursor="abc123")
    params = next_request_params(cfg, state)
    assert params == {"after": "abc123"}


def test_next_params_offset():
    cfg = PaginationConfig(strategy="offset", page_size=100)
    state = PaginationState(has_more=True, next_offset=200)
    params = next_request_params(cfg, state, {"extra": "value"})
    assert params["offset"] == 200
    assert params["limit"] == 100
    assert params["extra"] == "value"


def test_next_params_page_number():
    cfg = PaginationConfig(strategy="page_number", page_size=50)
    state = PaginationState(has_more=True, next_page=3)
    params = next_request_params(cfg, state)
    assert params["p"] == 3
    assert params["ps"] == 50


def test_next_params_exhausted():
    cfg = PaginationConfig(strategy="cursor")
    state = PaginationState(has_more=False)
    assert next_request_params(cfg, state) is None


def test_next_params_none_strategy():
    cfg = PaginationConfig(strategy="none")
    state = PaginationState(has_more=True)
    assert next_request_params(cfg, state) is None
