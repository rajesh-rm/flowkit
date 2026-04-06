"""Unit tests for runner.py pure-logic functions (no DB required)."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext
from data_assets.runner import (
    _check_row_count_anomaly,
    _check_source_freshness,
    _compute_date_window,
    _update_watermarks,
    run_asset,
)


# ---------------------------------------------------------------------------
# _compute_date_window
# ---------------------------------------------------------------------------


class TestComputeDateWindow:
    def test_full_mode_returns_none_none(self):
        start, end = _compute_date_window(RunMode.FULL, None, {})
        assert start is None
        assert end is None

    def test_transform_mode_returns_none_none(self):
        start, end = _compute_date_window(RunMode.TRANSFORM, None, {})
        assert start is None
        assert end is None

    def test_forward_mode_with_coverage(self):
        coverage = MagicMock()
        coverage.forward_watermark = datetime(2025, 6, 1, tzinfo=UTC)
        start, end = _compute_date_window(RunMode.FORWARD, coverage, {})
        assert start == datetime(2025, 6, 1, tzinfo=UTC)
        assert end is not None  # now()

    def test_forward_mode_without_coverage(self):
        start, end = _compute_date_window(RunMode.FORWARD, None, {})
        assert start is None
        assert end is not None

    def test_backfill_mode_with_coverage(self):
        coverage = MagicMock()
        coverage.backward_watermark = datetime(2025, 1, 1, tzinfo=UTC)
        start, end = _compute_date_window(RunMode.BACKFILL, coverage, {})
        assert start is None
        assert end == datetime(2025, 1, 1, tzinfo=UTC)

    def test_backfill_mode_without_coverage(self):
        start, end = _compute_date_window(RunMode.BACKFILL, None, {})
        assert start is None
        assert end is not None  # defaults to now

    def test_overrides_take_precedence(self):
        s = datetime(2025, 3, 1, tzinfo=UTC)
        e = datetime(2025, 3, 31, tzinfo=UTC)
        start, end = _compute_date_window(
            RunMode.FORWARD, None, {"start_date": s, "end_date": e}
        )
        assert start == s
        assert end == e


# ---------------------------------------------------------------------------
# _update_watermarks
# ---------------------------------------------------------------------------


class TestUpdateWatermarks:
    def test_skips_when_date_column_is_none(self):
        asset = MagicMock()
        asset.date_column = None
        with patch("data_assets.runner.update_coverage") as mock:
            _update_watermarks(MagicMock(), asset, RunMode.FULL, pd.DataFrame())
        mock.assert_not_called()

    def test_skips_when_column_not_in_df(self):
        asset = MagicMock()
        asset.date_column = "updated_at"
        df = pd.DataFrame({"id": [1, 2]})
        with patch("data_assets.runner.update_coverage") as mock:
            _update_watermarks(MagicMock(), asset, RunMode.FULL, df)
        mock.assert_not_called()

    def test_forward_mode_updates_forward_watermark(self):
        asset = MagicMock()
        asset.date_column = "updated_at"
        asset.name = "test"
        df = pd.DataFrame({"updated_at": ["2025-06-01T00:00:00Z", "2025-06-15T00:00:00Z"]})
        with patch("data_assets.runner.update_coverage") as mock:
            _update_watermarks(MagicMock(), asset, RunMode.FORWARD, df)
        mock.assert_called_once()
        call_kwargs = mock.call_args
        assert call_kwargs[1]["forward_watermark"] is not None

    def test_backfill_mode_updates_backward_watermark(self):
        asset = MagicMock()
        asset.date_column = "updated_at"
        asset.name = "test"
        df = pd.DataFrame({"updated_at": ["2025-01-01T00:00:00Z", "2025-01-15T00:00:00Z"]})
        with patch("data_assets.runner.update_coverage") as mock:
            _update_watermarks(MagicMock(), asset, RunMode.BACKFILL, df)
        mock.assert_called_once()
        call_kwargs = mock.call_args
        assert call_kwargs[1]["backward_watermark"] is not None

    def test_full_mode_updates_both_watermarks(self):
        asset = MagicMock()
        asset.date_column = "updated_at"
        asset.name = "test"
        df = pd.DataFrame({"updated_at": ["2025-01-01T00:00:00Z", "2025-06-15T00:00:00Z"]})
        with patch("data_assets.runner.update_coverage") as mock:
            _update_watermarks(MagicMock(), asset, RunMode.FULL, df)
        assert mock.call_count == 2


# ---------------------------------------------------------------------------
# Secret injection / cleanup
# ---------------------------------------------------------------------------


class TestSecretInjection:
    @patch("data_assets.runner._ensure_initialized")
    @patch("data_assets.runner.get_engine")
    def test_secrets_injected_and_cleaned_up(self, mock_engine, mock_init):
        """Secrets should be set as env vars during the run and removed after."""
        mock_engine.return_value = MagicMock()

        # Force a controlled failure so we don't need to mock the full lifecycle
        with patch("data_assets.runner.get", side_effect=KeyError("no_asset")):
            with pytest.raises(KeyError):
                run_asset(
                    "no_asset",
                    secrets={"MY_SECRET_KEY": "secret_value"},
                )

        # Secret should be cleaned up even after failure
        assert "MY_SECRET_KEY" not in os.environ

    @patch("data_assets.runner._ensure_initialized")
    @patch("data_assets.runner.get_engine")
    def test_secrets_available_during_run(self, mock_engine, mock_init):
        """Secrets should be available as env vars when asset code runs."""
        mock_engine.return_value = MagicMock()
        captured_value = {}

        def capture_get(name):
            captured_value["secret"] = os.environ.get("MY_TEST_SECRET")
            raise KeyError("stop")

        with patch("data_assets.runner.get", side_effect=capture_get):
            with pytest.raises(KeyError):
                run_asset("x", secrets={"MY_TEST_SECRET": "hello"})

        assert captured_value["secret"] == "hello"
        assert "MY_TEST_SECRET" not in os.environ


# ---------------------------------------------------------------------------
# _check_row_count_anomaly (pure logic, mock the DB query)
# ---------------------------------------------------------------------------


class TestCheckRowCountAnomaly:
    @patch("data_assets.runner.Session")
    def test_warns_on_low_count(self, mock_session_cls):
        session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
        session.execute.return_value.scalar.return_value = 100.0  # avg of last 5

        with patch("data_assets.runner.logger") as mock_logger:
            _check_row_count_anomaly(MagicMock(), "test_asset", 40)  # 40 < 50% of 100
        mock_logger.warning.assert_called_once()

    @patch("data_assets.runner.Session")
    def test_no_warning_on_normal_count(self, mock_session_cls):
        session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
        session.execute.return_value.scalar.return_value = 100.0

        with patch("data_assets.runner.logger") as mock_logger:
            _check_row_count_anomaly(MagicMock(), "test_asset", 80)
        mock_logger.warning.assert_not_called()

    @patch("data_assets.runner.Session")
    def test_no_warning_when_no_history(self, mock_session_cls):
        session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
        session.execute.return_value.scalar.return_value = None  # no prior runs

        with patch("data_assets.runner.logger") as mock_logger:
            _check_row_count_anomaly(MagicMock(), "test_asset", 5)
        mock_logger.warning.assert_not_called()


# ---------------------------------------------------------------------------
# _check_source_freshness
# ---------------------------------------------------------------------------


class TestCheckSourceFreshness:
    @patch("data_assets.runner.Session")
    def test_warns_on_stale_source(self, mock_session_cls):
        session = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
        # Source table last loaded 48 hours ago
        session.execute.return_value.scalar.return_value = datetime(
            2025, 1, 1, tzinfo=UTC
        )

        asset = MagicMock()
        asset.name = "test_transform"
        asset.source_tables = ["source_tbl"]

        with patch("data_assets.runner.logger") as mock_logger:
            _check_source_freshness(MagicMock(), asset, max_stale_hours=24)
        mock_logger.warning.assert_called_once()

    def test_skips_when_no_source_tables(self):
        asset = MagicMock()
        asset.source_tables = []
        # Should not raise or call anything
        _check_source_freshness(MagicMock(), asset)

    @patch("data_assets.runner.Session")
    def test_db_error_logged_as_warning_not_swallowed(self, mock_session_cls):
        """If the freshness check itself fails, it should log WARNING not DEBUG."""
        mock_session_cls.return_value.__enter__ = MagicMock(
            side_effect=RuntimeError("db down")
        )
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

        asset = MagicMock()
        asset.name = "test_transform"
        asset.source_tables = ["source_tbl"]

        with patch("data_assets.runner.logger") as mock_logger:
            _check_source_freshness(MagicMock(), asset)
        mock_logger.warning.assert_called_once()


# ---------------------------------------------------------------------------
# _check_row_count_anomaly — failure logging
# ---------------------------------------------------------------------------


class TestRowCountAnomalyFailureLogging:
    @patch("data_assets.runner.Session")
    def test_db_error_logged_as_warning(self, mock_session_cls):
        """If the anomaly check itself fails, it should log WARNING not DEBUG."""
        mock_session_cls.return_value.__enter__ = MagicMock(
            side_effect=RuntimeError("db down")
        )
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

        with patch("data_assets.runner.logger") as mock_logger:
            _check_row_count_anomaly(MagicMock(), "test_asset", 100)
        mock_logger.warning.assert_called_once()


# ---------------------------------------------------------------------------
# _update_watermarks — unparseable date warning
# ---------------------------------------------------------------------------


class TestUpdateWatermarksDateParsing:
    def test_warns_on_unparseable_dates(self):
        """Dates that can't be parsed should produce a WARNING, not be silently dropped."""
        asset = MagicMock()
        asset.date_column = "updated_at"
        asset.name = "test"
        df = pd.DataFrame({
            "updated_at": ["2025-01-01T00:00:00Z", "not-a-date", "also-bad"]
        })
        with patch("data_assets.runner.update_coverage"):
            with patch("data_assets.runner.logger") as mock_logger:
                _update_watermarks(MagicMock(), asset, RunMode.FULL, df)
        # Should warn about 2 unparseable dates
        mock_logger.warning.assert_called_once()
        assert "2 of 3" in mock_logger.warning.call_args[0][1] or \
               "2" in str(mock_logger.warning.call_args)

    def test_no_warning_when_all_dates_valid(self):
        asset = MagicMock()
        asset.date_column = "updated_at"
        asset.name = "test"
        df = pd.DataFrame({
            "updated_at": ["2025-01-01T00:00:00Z", "2025-06-15T00:00:00Z"]
        })
        with patch("data_assets.runner.update_coverage"):
            with patch("data_assets.runner.logger") as mock_logger:
                _update_watermarks(MagicMock(), asset, RunMode.FULL, df)
        mock_logger.warning.assert_not_called()
