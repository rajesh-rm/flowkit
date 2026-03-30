"""Unit tests for ServiceNow asset build_request/parse_response."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "servicenow"


def _ctx(**kwargs):
    return RunContext(
        run_id=uuid.uuid4(), mode=RunMode.FULL, asset_name="test", **kwargs
    )


def test_incidents_build_request(monkeypatch):
    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    asset = ServiceNowIncidents()
    spec = asset.build_request(_ctx())
    assert spec.url == "https://dev.service-now.com/api/now/table/incident"
    assert spec.params["sysparm_offset"] == 0
    assert spec.params["sysparm_limit"] == 100


def test_incidents_build_request_with_date(monkeypatch):
    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    asset = ServiceNowIncidents()
    ctx = _ctx(start_date=datetime(2025, 1, 1, tzinfo=timezone.utc))
    spec = asset.build_request(ctx)
    assert "sysparm_query" in spec.params
    assert "sys_updated_on>=" in spec.params["sysparm_query"]


def test_incidents_parse_response(monkeypatch):
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    data = json.loads((FIXTURES / "incidents.json").read_text())
    asset = ServiceNowIncidents()
    asset._current_offset = 0
    df, state = asset.parse_response(data)

    assert len(df) == 2
    assert "sys_id" in df.columns
    assert "number" in df.columns
    assert not state.has_more  # 2 results < page_size 100
    assert state.next_offset == 2


def test_incidents_parse_empty_response(monkeypatch):
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    asset = ServiceNowIncidents()
    asset._current_offset = 100
    df, state = asset.parse_response({"result": []})
    assert len(df) == 0
    assert not state.has_more


def test_changes_parse_response(monkeypatch):
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.changes import ServiceNowChanges

    data = json.loads((FIXTURES / "changes.json").read_text())
    asset = ServiceNowChanges()
    asset._current_offset = 0
    df, state = asset.parse_response(data)

    assert len(df) == 1
    assert df.iloc[0]["number"] == "CHG0001001"
    assert not state.has_more
