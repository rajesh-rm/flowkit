"""End-to-end tests for ServiceNow assets with mocked API and real Postgres."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pandas as pd
import pytest
import respx

from data_assets.core.registry import _registry


def _clear_registry():
    _registry.clear()


def _register_servicenow_assets():
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents
    from data_assets.assets.servicenow.changes import ServiceNowChanges


SNOW_URL = "https://dev12345.service-now.com"


@pytest.mark.integration
@respx.mock
def test_servicenow_incidents_full_run(clean_db, monkeypatch, load_fixture):
    """Full lifecycle: extract incidents, validate, promote."""
    _clear_registry()
    monkeypatch.setenv("SERVICENOW_INSTANCE", SNOW_URL)
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "test-pass")

    _register_servicenow_assets()

    incidents_data = load_fixture("servicenow/incidents.json")

    # First call returns data, second call returns empty (pagination stops)
    respx.get(f"{SNOW_URL}/api/now/table/incident").mock(
        side_effect=[
            httpx.Response(200, json=incidents_data),
            httpx.Response(200, json={"result": []}),
        ]
    )

    with patch("data_assets.runner.get_engine", return_value=clean_db):
        with patch("data_assets.db.engine.get_engine", return_value=clean_db):
            from data_assets.runner import run_asset
            result = run_asset("servicenow_incidents", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_loaded"] == 2

    df = pd.read_sql("SELECT * FROM raw.servicenow_incidents ORDER BY number", clean_db)
    assert len(df) == 2
    assert "INC0010001" in df["number"].values
    assert "INC0010002" in df["number"].values


@pytest.mark.integration
@respx.mock
def test_servicenow_changes_full_run(clean_db, monkeypatch, load_fixture):
    """Full lifecycle: extract change requests, validate, promote."""
    _clear_registry()
    monkeypatch.setenv("SERVICENOW_INSTANCE", SNOW_URL)
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "test-pass")

    _register_servicenow_assets()

    changes_data = load_fixture("servicenow/changes.json")

    respx.get(f"{SNOW_URL}/api/now/table/change_request").mock(
        side_effect=[
            httpx.Response(200, json=changes_data),
            httpx.Response(200, json={"result": []}),
        ]
    )

    with patch("data_assets.runner.get_engine", return_value=clean_db):
        with patch("data_assets.db.engine.get_engine", return_value=clean_db):
            from data_assets.runner import run_asset
            result = run_asset("servicenow_changes", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_loaded"] == 1

    df = pd.read_sql("SELECT * FROM raw.servicenow_changes", clean_db)
    assert len(df) == 1
    assert df.iloc[0]["number"] == "CHG0001001"
