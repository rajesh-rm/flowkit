"""Tests for parallel extraction: _fetch_pages, _resume_info, _run_workers, modes."""

from __future__ import annotations

import uuid
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext
from data_assets.core.types import (
    PaginationConfig,
    PaginationState,
    RequestSpec,
    SkippedRequestError,
)
from data_assets.extract.parallel import (
    _fetch_pages,
    _resume_info,
    _run_workers,
)


def _ctx():
    return RunContext(
        run_id=uuid.uuid4(), mode=RunMode.FULL, asset_name="test"
    )


# ---------------------------------------------------------------------------
# _resume_info
# ---------------------------------------------------------------------------

def test_resume_info_no_checkpoint():
    skip, rows, cp = _resume_info({}, "worker_0")
    assert skip is False
    assert rows == 0
    assert cp is None


def test_resume_info_completed():
    cps = {"worker_0": {"status": "completed", "rows_so_far": 42}}
    skip, rows, cp = _resume_info(cps, "worker_0")
    assert skip is True
    assert rows == 42
    assert cp is None


def test_resume_info_in_progress():
    cps = {"worker_0": {
        "status": "in_progress",
        "rows_so_far": 10,
        "checkpoint_value": {"cursor": "abc"},
    }}
    skip, rows, cp = _resume_info(cps, "worker_0")
    assert skip is False
    assert rows == 10
    assert cp == {"cursor": "abc"}


# ---------------------------------------------------------------------------
# _fetch_pages
# ---------------------------------------------------------------------------

def test_fetch_pages_single_page():
    """One page, has_more=False → returns immediately."""
    asset = MagicMock()
    asset.name = "test_asset"
    asset.pagination_config = PaginationConfig(strategy="page_number")
    asset.parse_response.return_value = (
        pd.DataFrame({"id": [1, 2]}),
        PaginationState(has_more=False),
    )

    client = MagicMock()
    client.request.return_value = {"data": [1, 2]}

    with patch("data_assets.extract.parallel.write_to_temp", return_value=2) as mock_write:
        rows = _fetch_pages(
            asset, client, MagicMock(), "temp_tbl", _ctx(),
            worker_id="main",
            request_builder=lambda cp: RequestSpec(method="GET", url="http://test"),
        )

    assert rows == 2
    assert client.request.call_count == 1
    assert mock_write.call_count == 1


def test_fetch_pages_multiple_pages():
    """Two pages then stop."""
    asset = MagicMock()
    asset.name = "test_asset"
    asset.pagination_config = PaginationConfig(strategy="offset", page_size=10)
    asset.should_stop.return_value = False
    asset.parse_response.side_effect = [
        (pd.DataFrame({"id": [1, 2]}), PaginationState(has_more=True, next_offset=10)),
        (pd.DataFrame({"id": [3, 4]}), PaginationState(has_more=False)),
    ]

    client = MagicMock()
    client.request.side_effect = [{"page": 1}, {"page": 2}]

    with patch("data_assets.extract.parallel.write_to_temp", return_value=2):
        with patch("data_assets.extract.parallel.save_checkpoint"):
            rows = _fetch_pages(
                asset, client, MagicMock(), "temp_tbl", _ctx(),
                worker_id="main",
                request_builder=lambda cp: RequestSpec(method="GET", url="http://test"),
            )

    assert rows == 4
    assert client.request.call_count == 2


def test_fetch_pages_skipped_request():
    """SkippedRequestError (e.g., 404) → stops gracefully, returns 0 rows."""
    asset = MagicMock()
    asset.name = "test_asset"
    asset.pagination_config = PaginationConfig(strategy="none")

    client = MagicMock()
    client.request.side_effect = SkippedRequestError("404 Not Found")

    with patch("data_assets.extract.parallel.write_to_temp") as mock_write:
        rows = _fetch_pages(
            asset, client, MagicMock(), "temp_tbl", _ctx(),
            worker_id="main",
            request_builder=lambda cp: RequestSpec(method="GET", url="http://test"),
        )

    assert rows == 0
    assert mock_write.call_count == 0


def test_fetch_pages_resumes_from_checkpoint():
    """Passing initial_checkpoint should forward it to the first request_builder call."""
    calls = []

    def builder(cp):
        calls.append(cp)
        return RequestSpec(method="GET", url="http://test")

    asset = MagicMock()
    asset.name = "test_asset"
    asset.pagination_config = PaginationConfig(strategy="offset")
    asset.parse_response.return_value = (
        pd.DataFrame({"id": [5]}),
        PaginationState(has_more=False),
    )

    client = MagicMock()
    client.request.return_value = {}

    with patch("data_assets.extract.parallel.write_to_temp", return_value=1):
        _fetch_pages(
            asset, client, MagicMock(), "temp_tbl", _ctx(),
            worker_id="main",
            request_builder=builder,
            initial_checkpoint={"next_offset": 100},
        )

    # First call should receive the saved checkpoint
    assert calls[0] == {"next_offset": 100}


# ---------------------------------------------------------------------------
# _run_workers
# ---------------------------------------------------------------------------

def test_run_workers_single_unit():
    """One work unit, no threading needed."""

    def worker_fn(wid, data):
        return sum(data)

    total = _run_workers(
        work_units=[("w0", [1, 2, 3])],
        worker_fn=worker_fn,
        max_workers=4,
    )
    assert total == 6


def test_run_workers_multiple_units():
    """Multiple work units run in parallel."""

    def worker_fn(wid, data):
        return len(data)

    total = _run_workers(
        work_units=[("w0", [1, 2]), ("w1", [3, 4, 5])],
        worker_fn=worker_fn,
        max_workers=4,
    )
    assert total == 5  # 2 + 3


def test_run_workers_propagates_exception():
    """Worker exception propagates to caller."""

    def worker_fn(wid, data):
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        _run_workers(
            work_units=[("w0", [])],
            worker_fn=worker_fn,
            max_workers=1,
        )


# ---------------------------------------------------------------------------
# Entity-parallel: entity marked complete only after all pages succeed
# ---------------------------------------------------------------------------

def test_entity_not_marked_complete_on_partial_failure():
    """Critical bug fix: entity must NOT be in completed_entities if it failed mid-page.

    The old code added entity to completed_entities BEFORE confirming all pages.
    The new code uses _fetch_pages() which returns only after success, and the
    entity is added to completed AFTER _fetch_pages returns.
    """
    # _fetch_pages raises on second page → entity should not be "completed"
    asset = MagicMock()
    asset.name = "test_asset"
    asset.pagination_config = PaginationConfig(strategy="page_number")
    asset.should_stop.return_value = False

    # First page succeeds, second page fails
    asset.parse_response.side_effect = [
        (pd.DataFrame({"id": [1]}), PaginationState(has_more=True, next_page=2)),
        Exception("API down"),
    ]

    client = MagicMock()
    client.request.side_effect = [{"p": 1}, {"p": 2}]

    with patch("data_assets.extract.parallel.write_to_temp", return_value=1):
        with patch("data_assets.extract.parallel.save_checkpoint"):
            with pytest.raises(Exception, match="API down"):
                _fetch_pages(
                    asset, client, MagicMock(), "temp_tbl", _ctx(),
                    worker_id="entities_0",
                    request_builder=lambda cp: RequestSpec(
                        method="GET", url="http://test"
                    ),
                )

    # parse_response was called twice (first succeeded, second raised)
    assert asset.parse_response.call_count == 2
    # Key: _fetch_pages raised BEFORE returning, so the caller
    # (entity_worker) never adds this entity to completed_entities.
    # This is the bug fix — old code would have marked it complete.
