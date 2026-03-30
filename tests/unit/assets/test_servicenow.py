"""Unit tests for ServiceNow asset build_request/parse_response."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
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
    assert spec.params["sysparm_limit"] == 100
    assert "sysparm_orderby" in spec.params  # keyset sorts by sys_updated_on,sys_id


def test_incidents_build_request_with_date(monkeypatch):
    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    asset = ServiceNowIncidents()
    ctx = _ctx(start_date=datetime(2025, 1, 1, tzinfo=UTC))
    spec = asset.build_request(ctx)
    assert "sysparm_query" in spec.params
    assert "sys_updated_on>=" in spec.params["sysparm_query"]


def test_incidents_build_request_with_keyset_checkpoint(monkeypatch):
    """Keyset pagination: checkpoint contains cursor with last seen sys_updated_on + sys_id."""
    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    asset = ServiceNowIncidents()
    cursor = json.dumps({"sys_updated_on": "2025-12-01T10:00:00Z", "sys_id": "abc123"})
    spec = asset.build_request(_ctx(), checkpoint={"cursor": cursor})
    assert "sysparm_query" in spec.params
    assert "sys_id>abc123" in spec.params["sysparm_query"]


def test_incidents_parse_response(monkeypatch):
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    data = json.loads((FIXTURES / "incidents.json").read_text())
    asset = ServiceNowIncidents()
    df, state = asset.parse_response(data)

    assert len(df) == 2
    assert "sys_id" in df.columns
    assert "number" in df.columns
    assert not state.has_more  # 2 results < page_size 100
    # Keyset: cursor should contain last record's sys_updated_on + sys_id
    assert state.cursor is not None
    cursor_data = json.loads(state.cursor)
    assert "sys_updated_on" in cursor_data
    assert "sys_id" in cursor_data


def test_incidents_parse_empty_response(monkeypatch):
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.incidents import ServiceNowIncidents

    asset = ServiceNowIncidents()
    df, state = asset.parse_response({"result": []})
    assert len(df) == 0
    assert not state.has_more
    assert state.cursor is None


def test_changes_parse_response(monkeypatch):
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
    from data_assets.assets.servicenow.changes import ServiceNowChanges

    data = json.loads((FIXTURES / "changes.json").read_text())
    asset = ServiceNowChanges()
    df, state = asset.parse_response(data)

    assert len(df) == 1
    assert df.iloc[0]["number"] == "CHG0001001"
    assert not state.has_more
