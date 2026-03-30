"""Pagination strategy helpers for building sequential page requests."""

from __future__ import annotations

from data_assets.core.types import PaginationConfig, PaginationState


def next_request_params(
    config: PaginationConfig,
    state: PaginationState,
    current_params: dict | None = None,
) -> dict | None:
    """Compute the query params for the next page request.

    Returns None if pagination is exhausted.
    """
    if not state.has_more:
        return None

    params = dict(current_params or {})

    if config.strategy == "cursor":
        if state.cursor is None:
            return None
        params[config.cursor_field or "cursor"] = state.cursor

    elif config.strategy == "offset":
        offset = state.next_offset or 0
        params["offset"] = offset
        params["limit"] = config.page_size

    elif config.strategy == "page_number":
        page = state.next_page or 2
        params["p"] = page
        params["ps"] = config.page_size

    elif config.strategy == "date_window":
        # Date window pagination is handled by the asset's build_request()
        pass

    elif config.strategy == "none":
        return None

    return params


def initial_params(config: PaginationConfig) -> dict:
    """Build the initial pagination params for the first request."""
    params: dict = {}
    if config.strategy == "offset":
        params["offset"] = 0
        params["limit"] = config.page_size
    elif config.strategy == "page_number":
        params["p"] = 1
        params["ps"] = config.page_size
    return params
