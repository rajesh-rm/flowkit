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

    def test_skips_when_all_dates_unparseable(self):
        """If every date is unparseable, col is empty after dropna — should not crash."""
        asset = MagicMock()
        asset.date_column = "updated_at"
        asset.name = "test"
        df = pd.DataFrame({"updated_at": ["not-a-date", "also-bad"]})
        with patch("data_assets.runner.update_coverage") as mock_cov:
            _update_watermarks(MagicMock(), asset, RunMode.FULL, df)
        mock_cov.assert_not_called()


# ---------------------------------------------------------------------------
# _ensure_initialized — double-check lock
# ---------------------------------------------------------------------------

from data_assets.runner import _ensure_initialized


class TestEnsureInitialized:
    def test_double_check_lock_skips_second_call(self):
        """After first successful init, subsequent calls return immediately."""
        import data_assets.runner as runner_mod

        original = runner_mod._initialized
        try:
            runner_mod._initialized = False
            with patch("data_assets.runner.create_all_tables") as mock_create, \
                 patch("data_assets.runner.discover") as mock_discover, \
                 patch("data_assets.runner.register_asset_metadata") as mock_register, \
                 patch("data_assets.runner.all_assets", return_value=[]):
                engine = MagicMock()
                _ensure_initialized(engine)
                assert runner_mod._initialized is True
                mock_create.assert_called_once()

                # Second call should be a no-op (fast path)
                _ensure_initialized(engine)
                mock_create.assert_called_once()  # still only one call
        finally:
            runner_mod._initialized = original

    def test_already_initialized_returns_immediately(self):
        """When _initialized is True, should skip all work."""
        import data_assets.runner as runner_mod

        original = runner_mod._initialized
        try:
            runner_mod._initialized = True
            with patch("data_assets.runner.create_all_tables") as mock_create:
                _ensure_initialized(MagicMock())
            mock_create.assert_not_called()
        finally:
            runner_mod._initialized = original


# ---------------------------------------------------------------------------
# run_asset — exception handling (lines 253-265)
# ---------------------------------------------------------------------------


class TestRunAssetExceptionHandling:
    @patch("data_assets.runner.setup_logging")
    @patch("data_assets.runner.get_engine")
    @patch("data_assets.runner._ensure_initialized")
    @patch("data_assets.runner.get")
    @patch("data_assets.runner.temp_table_name", return_value="tmp_test_123")
    @patch("data_assets.runner.acquire_or_takeover", return_value=(None, None))
    @patch("data_assets.runner.get_coverage", return_value=None)
    @patch("data_assets.runner.checkpoints_by_worker", return_value={})
    @patch("data_assets.runner.get_checkpoints", return_value=[])
    @patch("data_assets.runner.record_run_start")
    @patch("data_assets.runner._prepare_temp_table", return_value="tmp_tbl")
    @patch("data_assets.runner._run_extraction", side_effect=RuntimeError("extract boom"))
    @patch("data_assets.runner.record_run_failure")
    @patch("data_assets.runner.drop_temp_table")
    @patch("data_assets.runner.release_lock")
    def test_exception_records_failure_and_cleans_up(
        self,
        mock_release,
        mock_drop_temp,
        mock_record_fail,
        mock_extraction,
        mock_prepare,
        mock_record_start,
        mock_get_cps,
        mock_cp_by_worker,
        mock_coverage,
        mock_acquire,
        mock_temp_name,
        mock_get_asset,
        mock_init,
        mock_engine_fn,
        mock_logging,
    ):
        """When extraction raises, run_asset should record failure, drop temp table, and release lock."""
        mock_engine = MagicMock()
        mock_engine_fn.return_value = mock_engine

        mock_asset_cls = MagicMock()
        mock_asset = MagicMock()
        mock_asset.stale_heartbeat_minutes = 20
        mock_asset.max_run_hours = 5
        mock_asset_cls.return_value = mock_asset
        mock_get_asset.return_value = mock_asset_cls

        with pytest.raises(RuntimeError, match="extract boom"):
            run_asset("test_asset", run_mode="full")

        mock_record_fail.assert_called()
        mock_drop_temp.assert_called_once_with(mock_engine, "tmp_tbl")
        mock_release.assert_called_once_with(mock_engine, "test_asset", partition_key="")

    @patch("data_assets.runner.setup_logging")
    @patch("data_assets.runner.get_engine")
    @patch("data_assets.runner._ensure_initialized")
    @patch("data_assets.runner.get")
    @patch("data_assets.runner.temp_table_name", return_value="tmp_test_123")
    @patch("data_assets.runner.acquire_or_takeover", return_value=(None, None))
    @patch("data_assets.runner.get_coverage", return_value=None)
    @patch("data_assets.runner.checkpoints_by_worker", return_value={})
    @patch("data_assets.runner.get_checkpoints", return_value=[])
    @patch("data_assets.runner.record_run_start")
    @patch("data_assets.runner._prepare_temp_table", return_value="tmp_tbl")
    @patch("data_assets.runner._run_extraction", side_effect=RuntimeError("boom"))
    @patch("data_assets.runner.record_run_failure")
    @patch("data_assets.runner.drop_temp_table", side_effect=Exception("drop failed"))
    @patch("data_assets.runner.release_lock")
    def test_exception_cleanup_handles_drop_failure(
        self,
        mock_release,
        mock_drop_temp,
        mock_record_fail,
        mock_extraction,
        mock_prepare,
        mock_record_start,
        mock_get_cps,
        mock_cp_by_worker,
        mock_coverage,
        mock_acquire,
        mock_temp_name,
        mock_get_asset,
        mock_init,
        mock_engine_fn,
        mock_logging,
    ):
        """If drop_temp_table fails during cleanup, lock should still be released."""
        mock_engine = MagicMock()
        mock_engine_fn.return_value = mock_engine

        mock_asset_cls = MagicMock()
        mock_asset = MagicMock()
        mock_asset.stale_heartbeat_minutes = 20
        mock_asset.max_run_hours = 5
        mock_asset_cls.return_value = mock_asset
        mock_get_asset.return_value = mock_asset_cls

        with pytest.raises(RuntimeError, match="boom"):
            run_asset("test_asset", run_mode="full")

        mock_release.assert_called_once_with(mock_engine, "test_asset", partition_key="")


# ---------------------------------------------------------------------------
# _run_extraction — routing for TransformAsset and unknown types
# ---------------------------------------------------------------------------

from data_assets.runner import _run_extraction
from data_assets.core.asset import Asset
from data_assets.core.api_asset import APIAsset
from data_assets.core.transform_asset import TransformAsset


class _FakeTransformAsset(TransformAsset):
    """Minimal concrete TransformAsset for routing tests."""
    name = "fake_transform"
    target_table = "fake_transform"
    columns = []
    primary_key = ["id"]

    def query(self, context):
        return "SELECT 1"


class _FakeAPIAsset(APIAsset):
    """Minimal concrete APIAsset for routing tests."""
    name = "fake_api"
    target_table = "fake_api"
    columns = []
    primary_key = ["id"]

    def parse_response(self, response):
        return pd.DataFrame(), None


class TestRunExtractionRouting:
    @patch("data_assets.runner._check_source_freshness")
    @patch("data_assets.runner.execute_transform", return_value=42)
    def test_transform_asset_route(self, mock_exec, mock_freshness):
        """TransformAsset should route to execute_transform."""
        asset = _FakeTransformAsset()

        engine = MagicMock()
        context = MagicMock()

        rows, stats = _run_extraction(asset, engine, "tmp_tbl", context, {}, {})

        assert rows == 42
        assert stats == {}
        mock_freshness.assert_called_once()
        mock_exec.assert_called_once()

    def test_unknown_asset_type_raises(self):
        """An asset that is neither APIAsset nor TransformAsset should raise TypeError."""
        # Create a plain Asset subclass (not API or Transform)
        class _PlainAsset(Asset):
            name = "mystery"
            target_table = "mystery"
            columns = []
            primary_key = ["id"]

        asset = _PlainAsset()

        with pytest.raises(TypeError, match="mystery"):
            _run_extraction(asset, MagicMock(), "tmp", MagicMock(), {}, {})

    @patch("data_assets.runner._extract_api", return_value=(100, {"api_calls": 5}))
    def test_api_asset_route(self, mock_api):
        """APIAsset should route to _extract_api."""
        asset = _FakeAPIAsset()

        engine = MagicMock()
        context = MagicMock()

        rows, stats = _run_extraction(asset, engine, "tmp", context, {}, {})

        assert rows == 100
        assert stats == {"api_calls": 5}
        mock_api.assert_called_once()


# ---------------------------------------------------------------------------
# _run_transform_and_validate — custom transform path (lines 351-358)
# ---------------------------------------------------------------------------

from data_assets.runner import _run_transform_and_validate


class TestRunTransformAndValidateCustomTransform:
    @patch("data_assets.runner.read_temp_table")
    @patch("data_assets.runner.drop_table")
    @patch("data_assets.runner.create_temp_table", return_value="new_tmp")
    @patch("data_assets.runner.write_to_temp")
    def test_custom_transform_replaces_temp_table(
        self, mock_write, mock_create, mock_drop, mock_read
    ):
        """When asset has custom transform, old temp is dropped and new one created."""
        from data_assets.core.asset import Asset
        from data_assets.core.column import Column
        from data_assets.validation.validators import ValidationResult

        df_input = pd.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        df_transformed = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
        mock_read.return_value = df_input

        # Create a real Asset subclass with a custom transform method
        class CustomTransformAsset(Asset):
            name = "test_custom"
            target_schema = "raw"
            target_table = "test_custom"
            columns = [Column("id", Integer()), Column("value", Text())]
            primary_key = ["id"]

            def transform(self, df):
                return df_transformed

            def validate(self, df, context):
                return ValidationResult(passed=True, failures=[])

            def validate_warnings(self, df, context):
                return []

        asset = CustomTransformAsset()
        engine = MagicMock()
        run_id = uuid.uuid4()

        df, temp_tbl, warnings = _run_transform_and_validate(
            asset, engine, "old_tmp", MagicMock(), run_id, "test_custom"
        )

        assert temp_tbl == "new_tmp"
        mock_drop.assert_called_once()
        mock_create.assert_called_once()
        mock_write.assert_called_once()
        pd.testing.assert_frame_equal(df, df_transformed)


# ---------------------------------------------------------------------------
# _extract_api — token_manager_class is None (line 409)
# ---------------------------------------------------------------------------

from data_assets.runner import _extract_api
from sqlalchemy import Integer, Text


class TestExtractApiTokenManagerNone:
    def test_raises_when_no_token_manager(self):
        """API asset without token_manager_class should raise ValueError."""
        asset = MagicMock(spec=APIAsset)
        asset.name = "no_token"
        asset.token_manager_class = None

        with pytest.raises(ValueError, match="token_manager_class"):
            _extract_api(asset, MagicMock(), "tmp", MagicMock(), {}, {})


# ---------------------------------------------------------------------------
# _compute_date_window — fallback (line 486)
# ---------------------------------------------------------------------------


class TestComputeDateWindowFallback:
    def test_unknown_mode_returns_none_none(self):
        """A hypothetical unrecognized mode should hit the fallback return None, None.

        This is a defensive test — all known modes are covered above, but
        the fallback line 486 needs coverage.
        """
        # Create a mock mode that doesn't match any known branch
        mock_mode = MagicMock()
        mock_mode.__eq__ = lambda self, other: False
        start, end = _compute_date_window(mock_mode, None, {})
        assert start is None
        assert end is None


# ---------------------------------------------------------------------------
# _update_watermarks — empty column after dropna (line 507)
# ---------------------------------------------------------------------------


class TestUpdateWatermarksEmptyAfterDropna:
    def test_returns_early_when_all_dates_null(self):
        """If date column contains only NaN/None, col.empty is True after dropna."""
        asset = MagicMock()
        asset.date_column = "updated_at"
        asset.name = "test"
        df = pd.DataFrame({"updated_at": [None, None]})
        with patch("data_assets.runner.update_coverage") as mock_cov:
            _update_watermarks(MagicMock(), asset, RunMode.FULL, df)
        mock_cov.assert_not_called()


# ---------------------------------------------------------------------------
# partition_key threading
# ---------------------------------------------------------------------------


class TestPartitionKeyInRunContext:
    """Verify that partition_key is properly threaded through the run lifecycle."""

    def test_run_context_carries_partition_key(self):
        """RunContext dataclass stores partition_key with default empty string."""
        ctx = RunContext(
            run_id=uuid.uuid4(),
            mode=RunMode.FULL,
            asset_name="test",
        )
        assert ctx.partition_key == ""

    def test_run_context_carries_custom_partition_key(self):
        """RunContext dataclass stores a non-default partition_key."""
        ctx = RunContext(
            run_id=uuid.uuid4(),
            mode=RunMode.FULL,
            asset_name="test",
            partition_key="org-acme",
        )
        assert ctx.partition_key == "org-acme"

    @patch("data_assets.runner.setup_logging")
    @patch("data_assets.runner.get_engine")
    @patch("data_assets.runner._ensure_initialized")
    @patch("data_assets.runner.get")
    @patch("data_assets.runner.temp_table_name", return_value="tmp_test_123")
    @patch("data_assets.runner.acquire_or_takeover", return_value=(None, None))
    @patch("data_assets.runner.get_coverage", return_value=None)
    @patch("data_assets.runner.checkpoints_by_worker", return_value={})
    @patch("data_assets.runner.get_checkpoints", return_value=[])
    @patch("data_assets.runner.record_run_start")
    @patch("data_assets.runner._prepare_temp_table", return_value="tmp_tbl")
    @patch("data_assets.runner._run_extraction", return_value=(10, {}))
    @patch("data_assets.runner._run_transform_and_validate")
    @patch("data_assets.runner._run_promotion", return_value=10)
    @patch("data_assets.runner._update_watermarks")
    @patch("data_assets.runner.update_last_success")
    @patch("data_assets.runner.record_run_success")
    @patch("data_assets.runner.clear_checkpoints")
    @patch("data_assets.runner.drop_temp_table")
    @patch("data_assets.runner.release_lock")
    def test_partition_key_in_return_dict(
        self,
        mock_release,
        mock_drop_temp,
        mock_clear_cp,
        mock_record_success,
        mock_update_last,
        mock_update_wm,
        mock_promotion,
        mock_transform,
        mock_extraction,
        mock_prepare,
        mock_record_start,
        mock_get_cps,
        mock_cp_by_worker,
        mock_coverage,
        mock_acquire,
        mock_temp_name,
        mock_get_asset,
        mock_init,
        mock_engine_fn,
        mock_logging,
    ):
        """run_asset() return dict should include partition_key."""
        mock_engine = MagicMock()
        mock_engine_fn.return_value = mock_engine

        mock_asset_cls = MagicMock()
        mock_asset = MagicMock()
        mock_asset.stale_heartbeat_minutes = 20
        mock_asset.max_run_hours = 5
        mock_asset_cls.return_value = mock_asset
        mock_get_asset.return_value = mock_asset_cls

        mock_transform.return_value = (pd.DataFrame(), "tmp_tbl", [])

        result = run_asset("test_asset", run_mode="full")

        assert "partition_key" in result
        assert result["partition_key"] == ""

    @patch("data_assets.runner.setup_logging")
    @patch("data_assets.runner.get_engine")
    @patch("data_assets.runner._ensure_initialized")
    @patch("data_assets.runner.get")
    @patch("data_assets.runner.temp_table_name", return_value="tmp_test_123")
    @patch("data_assets.runner.acquire_or_takeover", return_value=(None, None))
    @patch("data_assets.runner.get_coverage", return_value=None)
    @patch("data_assets.runner.checkpoints_by_worker", return_value={})
    @patch("data_assets.runner.get_checkpoints", return_value=[])
    @patch("data_assets.runner.record_run_start")
    @patch("data_assets.runner._prepare_temp_table", return_value="tmp_tbl")
    @patch("data_assets.runner._run_extraction", return_value=(10, {}))
    @patch("data_assets.runner._run_transform_and_validate")
    @patch("data_assets.runner._run_promotion", return_value=10)
    @patch("data_assets.runner._update_watermarks")
    @patch("data_assets.runner.update_last_success")
    @patch("data_assets.runner.record_run_success")
    @patch("data_assets.runner.clear_checkpoints")
    @patch("data_assets.runner.drop_temp_table")
    @patch("data_assets.runner.release_lock")
    def test_custom_partition_key_threaded_through(
        self,
        mock_release,
        mock_drop_temp,
        mock_clear_cp,
        mock_record_success,
        mock_update_last,
        mock_update_wm,
        mock_promotion,
        mock_transform,
        mock_extraction,
        mock_prepare,
        mock_record_start,
        mock_get_cps,
        mock_cp_by_worker,
        mock_coverage,
        mock_acquire,
        mock_temp_name,
        mock_get_asset,
        mock_init,
        mock_engine_fn,
        mock_logging,
    ):
        """A non-default partition_key should flow through to all partition-aware calls."""
        mock_engine = MagicMock()
        mock_engine_fn.return_value = mock_engine

        mock_asset_cls = MagicMock()
        mock_asset = MagicMock()
        mock_asset.stale_heartbeat_minutes = 20
        mock_asset.max_run_hours = 5
        mock_asset_cls.return_value = mock_asset
        mock_get_asset.return_value = mock_asset_cls

        mock_transform.return_value = (pd.DataFrame(), "tmp_tbl", [])

        result = run_asset("test_asset", run_mode="full", partition_key="org-acme")

        # Return dict should carry the partition_key
        assert result["partition_key"] == "org-acme"

        # All partition-aware functions should receive partition_key="org-acme"
        mock_acquire.assert_called_once()
        assert mock_acquire.call_args[1]["partition_key"] == "org-acme"

        mock_coverage.assert_called_once_with(mock_engine, "test_asset", partition_key="org-acme")

        mock_get_cps.assert_called_once_with(mock_engine, "test_asset", partition_key="org-acme")

        mock_record_start.assert_called_once()
        assert mock_record_start.call_args[1]["partition_key"] == "org-acme"

        mock_prepare.assert_called_once()
        assert mock_prepare.call_args[1]["partition_key"] == "org-acme"

        mock_update_wm.assert_called_once()
        assert mock_update_wm.call_args[1]["partition_key"] == "org-acme"

        mock_clear_cp.assert_called_once_with(mock_engine, "test_asset", partition_key="org-acme")

        mock_release.assert_called_once_with(mock_engine, "test_asset", partition_key="org-acme")
