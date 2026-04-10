"""Tests for fingerprint-based drift detection in run_asset()."""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from data_assets.core.asset import Asset
from data_assets.core.column import Column, Index
from data_assets.core.enums import RunMode
from data_assets.core.registry import register
from data_assets.dag.fingerprint import compute_fingerprint
from sqlalchemy import Text


class _DriftAsset(Asset):
    name = "drift_test"
    target_table = "drift_test"
    default_run_mode = RunMode.FULL
    columns = [Column("id", Text())]
    primary_key = ["id"]
    indexes = [Index(columns=["id"])]


@pytest.fixture(autouse=True)
def _register_drift_asset():
    register(_DriftAsset)


@patch("data_assets.runner.get_engine")
@patch("data_assets.runner._ensure_initialized")
@patch("data_assets.runner.acquire_or_takeover", return_value=("temp_tbl", None))
@patch("data_assets.runner.record_run_start")
@patch("data_assets.runner.create_temp_table")
@patch("data_assets.runner.temp_table_name", return_value="temp_tbl")
def test_matching_fingerprint_no_warning(
    mock_temp_name, mock_create, mock_start, mock_acquire,
    mock_init, mock_engine, caplog,
):
    """When fingerprint matches, no warning should be logged."""
    fp = compute_fingerprint(_DriftAsset)

    # run_asset will fail later in the pipeline (no real DB), but the
    # fingerprint check happens early — we just need to verify the log
    with caplog.at_level(logging.WARNING, logger="data_assets.runner"):
        try:
            from data_assets.runner import run_asset
            run_asset("drift_test", asset_fingerprint=fp)
        except Exception:
            pass

    assert "fingerprint mismatch" not in caplog.text.lower()


@patch("data_assets.runner.get_engine")
@patch("data_assets.runner._ensure_initialized")
@patch("data_assets.runner.acquire_or_takeover", return_value=("temp_tbl", None))
@patch("data_assets.runner.record_run_start")
@patch("data_assets.runner.create_temp_table")
@patch("data_assets.runner.temp_table_name", return_value="temp_tbl")
def test_mismatched_fingerprint_warning(
    mock_temp_name, mock_create, mock_start, mock_acquire,
    mock_init, mock_engine, caplog,
):
    """When fingerprint doesn't match, a warning should be logged."""
    with caplog.at_level(logging.WARNING, logger="data_assets.runner"):
        try:
            from data_assets.runner import run_asset
            run_asset("drift_test", asset_fingerprint="0000000000000000")
        except Exception:
            pass

    assert "dag fingerprint mismatch" in caplog.text.lower()


@patch("data_assets.runner.get_engine")
@patch("data_assets.runner._ensure_initialized")
@patch("data_assets.runner.acquire_or_takeover", return_value=("temp_tbl", None))
@patch("data_assets.runner.record_run_start")
@patch("data_assets.runner.create_temp_table")
@patch("data_assets.runner.temp_table_name", return_value="temp_tbl")
def test_no_fingerprint_no_validation(
    mock_temp_name, mock_create, mock_start, mock_acquire,
    mock_init, mock_engine, caplog,
):
    """When no fingerprint is provided, no validation occurs (backward compat)."""
    with caplog.at_level(logging.WARNING, logger="data_assets.runner"):
        try:
            from data_assets.runner import run_asset
            run_asset("drift_test")
        except Exception:
            pass

    assert "fingerprint" not in caplog.text.lower()
