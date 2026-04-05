"""Unit tests for ServiceNow assets: all tables via shared base."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data_assets.core.enums import LoadStrategy, RunMode
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
        assert spec.params["sysparm_limit"] == 1000
        assert spec.params["sysparm_orderby"] == "sys_updated_on,sys_id"
        assert spec.params["sysparm_exclude_reference_link"] == "true"
        assert spec.params["sysparm_no_count"] == "true"

    def test_sysparm_fields_matches_columns(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        asset = ServiceNowIncidents()
        spec = asset.build_request(make_ctx())
        fields = spec.params["sysparm_fields"].split(",")
        column_names = [c.name for c in asset.columns]
        assert fields == column_names

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
        assert asset.pagination_config.page_size == 1000
        assert asset.date_column == "sys_updated_on"


# ---------------------------------------------------------------------------
# pysnc client creation
# ---------------------------------------------------------------------------


class TestPysncClientCreation:
    def test_basic_auth(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        with patch("pysnc.ServiceNowClient") as mock_cls:
            ServiceNowIncidents()._create_pysnc_client()
            mock_cls.assert_called_once_with(
                "https://dev.service-now.com", ("admin", "pass"),
            )

    def test_oauth2_when_all_creds_set(self, servicenow_env, monkeypatch):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        monkeypatch.setenv("SERVICENOW_CLIENT_ID", "cid")
        monkeypatch.setenv("SERVICENOW_CLIENT_SECRET", "csec")

        with (
            patch("pysnc.ServiceNowClient") as mock_cls,
            patch("pysnc.auth.ServiceNowPasswordGrantFlow") as mock_flow,
        ):
            ServiceNowIncidents()._create_pysnc_client()
            mock_flow.assert_called_once_with("admin", "pass", "cid", "csec")
            mock_cls.assert_called_once()

    def test_missing_instance_raises(self, monkeypatch):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        monkeypatch.delenv("SERVICENOW_INSTANCE", raising=False)
        with pytest.raises(RuntimeError, match="SERVICENOW_INSTANCE"):
            ServiceNowIncidents()._create_pysnc_client()

    def test_missing_credentials_raises(self, monkeypatch):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
        with pytest.raises(RuntimeError, match="SERVICENOW_USERNAME"):
            ServiceNowIncidents()._create_pysnc_client()


# ---------------------------------------------------------------------------
# pysnc extract() method
# ---------------------------------------------------------------------------


class TestPysncExtract:
    def _make_mock_gr(self, records):
        """Create a mock GlideRecord that iterates over records."""
        gr = MagicMock()
        mock_records = []
        for rec in records:
            mock_record = MagicMock()
            mock_record.serialize.return_value = rec
            mock_records.append(mock_record)
        gr.__iter__ = MagicMock(return_value=iter(mock_records))
        return gr

    def test_extract_writes_to_temp_table(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        records = [
            {"sys_id": "a1", "number": "INC001", "state": "1",
             "sys_updated_on": "2025-12-01T10:00:00Z"},
            {"sys_id": "a2", "number": "INC002", "state": "2",
             "sys_updated_on": "2025-12-01T11:00:00Z"},
        ]
        mock_gr = self._make_mock_gr(records)
        mock_client = MagicMock()
        mock_client.GlideRecord.return_value = mock_gr

        asset = ServiceNowIncidents()
        engine = MagicMock()

        with (
            patch.object(asset, "_create_pysnc_client", return_value=mock_client),
            patch(
                "data_assets.assets.servicenow.base.write_to_temp", return_value=2,
            ) as mock_write,
        ):
            rows = asset.extract(engine, "temp_incidents", make_ctx())

        assert rows == 2
        mock_write.assert_called_once()
        df = mock_write.call_args[0][2]
        assert len(df) == 2
        assert list(df["sys_id"]) == ["a1", "a2"]

    def test_extract_applies_date_filter(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        mock_gr = self._make_mock_gr([])
        mock_client = MagicMock()
        mock_client.GlideRecord.return_value = mock_gr

        asset = ServiceNowIncidents()
        ctx = make_ctx(start_date=datetime(2025, 6, 1, tzinfo=UTC))

        with (
            patch.object(asset, "_create_pysnc_client", return_value=mock_client),
            patch("data_assets.assets.servicenow.base.write_to_temp", return_value=0),
        ):
            asset.extract(MagicMock(), "temp", ctx)

        mock_gr.add_query.assert_called_once_with(
            "sys_updated_on", ">=", "2025-06-01 00:00:00",
        )

    def test_extract_sets_fields(self, servicenow_env):
        from data_assets.assets.servicenow.incidents import ServiceNowIncidents

        mock_gr = self._make_mock_gr([])
        mock_client = MagicMock()
        mock_client.GlideRecord.return_value = mock_gr

        asset = ServiceNowIncidents()

        with (
            patch.object(asset, "_create_pysnc_client", return_value=mock_client),
            patch("data_assets.assets.servicenow.base.write_to_temp", return_value=0),
        ):
            asset.extract(MagicMock(), "temp", make_ctx())

        expected_fields = [c.name for c in asset.columns]
        assert mock_gr.fields == expected_fields

    def test_extract_returns_none_not_called_on_base_asset(self):
        """Base Asset.extract() returns None (standard pipeline)."""
        from data_assets.core.asset import Asset

        # Can't instantiate ABC, but can test the method exists
        assert Asset.extract(MagicMock(), MagicMock(), "t", make_ctx()) is None


# ---------------------------------------------------------------------------
# Credential validation
# ---------------------------------------------------------------------------


class TestServiceNowTokenManagerValidation:
    def test_missing_instance_raises(self):
        """ServiceNowTokenManager should fail fast if SERVICENOW_INSTANCE is missing."""
        import pytest  # noqa: E402

        from data_assets.extract.token_manager import ServiceNowTokenManager

        with pytest.raises(RuntimeError, match="SERVICENOW_INSTANCE"):
            ServiceNowTokenManager()

    def test_missing_all_credentials_raises(self, monkeypatch):
        """Should fail if neither OAuth nor basic auth credentials are set."""
        import pytest  # noqa: E402

        from data_assets.extract.token_manager import ServiceNowTokenManager

        monkeypatch.setenv("SERVICENOW_INSTANCE", "https://test.service-now.com")
        with pytest.raises(RuntimeError, match="SERVICENOW_CLIENT_ID"):
            ServiceNowTokenManager()


# ---------------------------------------------------------------------------
# New assets: Users
# ---------------------------------------------------------------------------


class TestServiceNowUsers:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.users import ServiceNowUsers

        assert ServiceNowUsers().table_name == "sys_user"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.users import ServiceNowUsers

        assert ServiceNowUsers().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.users import ServiceNowUsers

        data = json.loads((FIXTURES / "users.json").read_text())
        df, state = ServiceNowUsers().parse_response(data)
        assert len(df) == 2
        assert "user_name" in df.columns
        assert df.iloc[0]["user_name"] == "john.doe"
        assert not state.has_more

    def test_build_request_url(self, servicenow_env):
        from data_assets.assets.servicenow.users import ServiceNowUsers

        spec = ServiceNowUsers().build_request(make_ctx())
        assert spec.url.endswith("/api/now/table/sys_user")


# ---------------------------------------------------------------------------
# New assets: User Groups
# ---------------------------------------------------------------------------


class TestServiceNowUserGroups:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.user_groups import ServiceNowUserGroups

        assert ServiceNowUserGroups().table_name == "sys_user_group"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.user_groups import ServiceNowUserGroups

        assert ServiceNowUserGroups().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.user_groups import ServiceNowUserGroups

        data = json.loads((FIXTURES / "user_groups.json").read_text())
        df, state = ServiceNowUserGroups().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["name"] == "Network Team"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Locations
# ---------------------------------------------------------------------------


class TestServiceNowLocations:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.locations import ServiceNowLocations

        assert ServiceNowLocations().table_name == "cmn_location"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.locations import ServiceNowLocations

        assert ServiceNowLocations().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.locations import ServiceNowLocations

        data = json.loads((FIXTURES / "locations.json").read_text())
        df, state = ServiceNowLocations().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["city"] == "New York"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Departments
# ---------------------------------------------------------------------------


class TestServiceNowDepartments:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.departments import ServiceNowDepartments

        assert ServiceNowDepartments().table_name == "cmn_department"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.departments import ServiceNowDepartments

        assert ServiceNowDepartments().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.departments import ServiceNowDepartments

        data = json.loads((FIXTURES / "departments.json").read_text())
        df, state = ServiceNowDepartments().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Engineering"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: CMDB CIs
# ---------------------------------------------------------------------------


class TestServiceNowCmdbCIs:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.cmdb_cis import ServiceNowCmdbCIs

        assert ServiceNowCmdbCIs().table_name == "cmdb_ci"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.cmdb_cis import ServiceNowCmdbCIs

        assert ServiceNowCmdbCIs().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.cmdb_cis import ServiceNowCmdbCIs

        data = json.loads((FIXTURES / "cmdb_cis.json").read_text())
        df, state = ServiceNowCmdbCIs().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["name"] == "web-server-01"
        assert df.iloc[0]["sys_class_name"] == "cmdb_ci_server"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Hardware Assets
# ---------------------------------------------------------------------------


class TestServiceNowHardwareAssets:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.hardware_assets import (
            ServiceNowHardwareAssets,
        )

        assert ServiceNowHardwareAssets().table_name == "alm_hardware"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.hardware_assets import (
            ServiceNowHardwareAssets,
        )

        assert ServiceNowHardwareAssets().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.hardware_assets import (
            ServiceNowHardwareAssets,
        )

        data = json.loads((FIXTURES / "hardware_assets.json").read_text())
        df, state = ServiceNowHardwareAssets().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["display_name"] == "Dell PowerEdge R740"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Problems
# ---------------------------------------------------------------------------


class TestServiceNowProblems:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.problems import ServiceNowProblems

        assert ServiceNowProblems().table_name == "problem"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.problems import ServiceNowProblems

        assert ServiceNowProblems().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.problems import ServiceNowProblems

        data = json.loads((FIXTURES / "problems.json").read_text())
        df, state = ServiceNowProblems().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["number"] == "PRB0010001"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Change Tasks
# ---------------------------------------------------------------------------


class TestServiceNowChangeTasks:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.change_tasks import ServiceNowChangeTasks

        assert ServiceNowChangeTasks().table_name == "change_task"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.change_tasks import ServiceNowChangeTasks

        assert ServiceNowChangeTasks().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.change_tasks import ServiceNowChangeTasks

        data = json.loads((FIXTURES / "change_tasks.json").read_text())
        df, state = ServiceNowChangeTasks().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["number"] == "CTASK0010001"
        assert df.iloc[0]["change_request"] == "chg001abc"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Catalog Requests
# ---------------------------------------------------------------------------


class TestServiceNowCatalogRequests:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.catalog_requests import (
            ServiceNowCatalogRequests,
        )

        assert ServiceNowCatalogRequests().table_name == "sc_request"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.catalog_requests import (
            ServiceNowCatalogRequests,
        )

        assert ServiceNowCatalogRequests().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.catalog_requests import (
            ServiceNowCatalogRequests,
        )

        data = json.loads((FIXTURES / "catalog_requests.json").read_text())
        df, state = ServiceNowCatalogRequests().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["number"] == "REQ0010001"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Catalog Items
# ---------------------------------------------------------------------------


class TestServiceNowCatalogItems:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.catalog_items import ServiceNowCatalogItems

        assert ServiceNowCatalogItems().table_name == "sc_req_item"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.catalog_items import ServiceNowCatalogItems

        assert ServiceNowCatalogItems().primary_key == ["sys_id"]

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.catalog_items import ServiceNowCatalogItems

        data = json.loads((FIXTURES / "catalog_items.json").read_text())
        df, state = ServiceNowCatalogItems().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["number"] == "RITM0010001"
        assert df.iloc[0]["quantity"] == "1"
        assert not state.has_more


# ---------------------------------------------------------------------------
# New assets: Choices (special — FULL_REPLACE, no incremental)
# ---------------------------------------------------------------------------


class TestServiceNowChoices:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow.choices import ServiceNowChoices

        assert ServiceNowChoices().table_name == "sys_choice"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow.choices import ServiceNowChoices

        assert ServiceNowChoices().primary_key == ["sys_id"]

    def test_load_strategy(self, servicenow_env):
        from data_assets.assets.servicenow.choices import ServiceNowChoices

        assert ServiceNowChoices().load_strategy == LoadStrategy.FULL_REPLACE

    def test_default_run_mode(self, servicenow_env):
        from data_assets.assets.servicenow.choices import ServiceNowChoices

        assert ServiceNowChoices().default_run_mode == RunMode.FULL

    def test_date_column_none(self, servicenow_env):
        from data_assets.assets.servicenow.choices import ServiceNowChoices

        assert ServiceNowChoices().date_column is None

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow.choices import ServiceNowChoices

        data = json.loads((FIXTURES / "choices.json").read_text())
        df, state = ServiceNowChoices().parse_response(data)
        assert len(df) == 3
        assert df.iloc[0]["label"] == "New"
        assert df.iloc[0]["name"] == "incident"
        assert df.iloc[0]["element"] == "state"
        assert not state.has_more

    def test_build_request_no_date_filter(self, servicenow_env):
        """FULL mode should not add a date filter to the query."""
        from data_assets.assets.servicenow.choices import ServiceNowChoices

        spec = ServiceNowChoices().build_request(make_ctx())
        assert "sysparm_query" not in spec.params
