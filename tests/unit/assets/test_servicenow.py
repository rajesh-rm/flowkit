"""Unit tests for ServiceNow assets: incidents and changes (via shared base)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "servicenow"


# ---------------------------------------------------------------------------
# ServiceNow Incidents
# ---------------------------------------------------------------------------


class TestServiceNowIncidentsBuildRequest:
    def test_basic(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        spec = ServiceNowIncidents().build_request(make_ctx())
        assert spec.url == "https://dev.service-now.com/api/now/table/incident"
        assert spec.params["sysparm_limit"] == 100
        assert spec.params["sysparm_orderby"] == "sys_updated_on,sys_id"

    def test_with_start_date(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        ctx = make_ctx(start_date=datetime(2025, 1, 1, tzinfo=UTC))
        spec = ServiceNowIncidents().build_request(ctx)
        assert "sys_updated_on>=" in spec.params["sysparm_query"]

    def test_with_keyset_checkpoint(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        cursor = json.dumps({
            "sys_updated_on": "2025-12-01T10:00:00Z",
            "sys_id": "abc123",
        })
        spec = ServiceNowIncidents().build_request(
            make_ctx(), checkpoint={"cursor": cursor}
        )
        query = spec.params["sysparm_query"]
        assert "sys_id>abc123" in query
        assert "sys_updated_on>=" in query


class TestServiceNowIncidentsParseResponse:
    def test_happy_path(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        data = json.loads((FIXTURES / "incidents.json").read_text())
        df, state = ServiceNowIncidents().parse_response(data)
        assert len(df) == 2
        assert "sys_id" in df.columns
        assert not state.has_more
        cursor_data = json.loads(state.cursor)
        assert "sys_updated_on" in cursor_data
        assert "sys_id" in cursor_data

    def test_empty_response(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        df, state = ServiceNowIncidents().parse_response({"result": []})
        assert len(df) == 0
        assert not state.has_more
        assert state.cursor is None

    def test_empty_response_has_column_schema(self, servicenow_env):
        """Empty response should still have correct column names."""
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        df, _ = ServiceNowIncidents().parse_response({"result": []})
        assert "sys_id" in df.columns
        assert "number" in df.columns


# ---------------------------------------------------------------------------
# ServiceNow Changes
# ---------------------------------------------------------------------------


class TestServiceNowChanges:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.changes import ServiceNowChanges

        assert ServiceNowChanges().table_name == "change_request"

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.changes import ServiceNowChanges

        data = json.loads((FIXTURES / "changes.json").read_text())
        df, state = ServiceNowChanges().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["number"] == "CHG0001001"
        assert not state.has_more


# ---------------------------------------------------------------------------
# Shared ServiceNowTableAsset behavior
# ---------------------------------------------------------------------------


class TestServiceNowTableAssetBase:
    def test_incidents_and_changes_share_base(self, servicenow_env):
        from data_assets.assets.servicenow.base import ServiceNowTableAsset
        from data_assets.assets.servicenow.changes import ServiceNowChanges
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        assert issubclass(ServiceNowIncidents, ServiceNowTableAsset)
        assert issubclass(ServiceNowChanges, ServiceNowTableAsset)

    def test_keyset_pagination_config(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        asset = ServiceNowIncidents()
        assert asset.pagination_config.strategy == "keyset"
        assert asset.date_column == "sys_updated_on"


# ---------------------------------------------------------------------------
# Credential validation
# ---------------------------------------------------------------------------


class TestServiceNowTokenManagerValidation:
    def test_missing_instance_raises(self):
        """ServiceNowTokenManager should fail fast if SERVICENOW_INSTANCE is missing."""
        import pytest
        from data_assets.extract.token_manager import ServiceNowTokenManager

        with pytest.raises(RuntimeError, match="SERVICENOW_INSTANCE"):
            ServiceNowTokenManager()

    def test_missing_all_credentials_raises(self, monkeypatch):
        """Should fail if neither OAuth nor basic auth credentials are set."""
        import pytest
        from data_assets.extract.token_manager import ServiceNowTokenManager

        monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
        with pytest.raises(RuntimeError, match="SERVICENOW_CLIENT_ID"):
            ServiceNowTokenManager()
