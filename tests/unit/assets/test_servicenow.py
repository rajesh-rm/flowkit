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
        from data_assets.assets.servicenow import ServiceNowIncidents

        spec = ServiceNowIncidents().build_request(make_ctx())
        assert spec.url == "https://dev.service-now.com/api/now/table/incident"
        assert spec.params["sysparm_limit"] == 1000
        assert spec.params["sysparm_orderby"] == "sys_updated_on,sys_id"
        assert spec.params["sysparm_exclude_reference_link"] == "true"
        assert spec.params["sysparm_no_count"] == "true"

    def test_sysparm_fields_matches_columns(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowIncidents

        asset = ServiceNowIncidents()
        spec = asset.build_request(make_ctx())
        fields = spec.params["sysparm_fields"].split(",")
        column_names = [c.name for c in asset.columns]
        assert fields == column_names

    def test_with_start_date(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowIncidents

        ctx = make_ctx(start_date=datetime(2025, 1, 1, tzinfo=UTC))
        spec = ServiceNowIncidents().build_request(ctx)
        assert "sys_updated_on>=" in spec.params["sysparm_query"]

    def test_with_keyset_checkpoint(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowIncidents

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
        from data_assets.assets.servicenow import ServiceNowIncidents

        data = json.loads((FIXTURES / "incidents.json").read_text())
        df, state = ServiceNowIncidents().parse_response(data)
        assert len(df) == 2
        assert "sys_id" in df.columns
        assert not state.has_more
        cursor_data = json.loads(state.cursor)
        assert "sys_updated_on" in cursor_data
        assert "sys_id" in cursor_data

    def test_empty_response(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowIncidents

        df, state = ServiceNowIncidents().parse_response({"result": []})
        assert len(df) == 0
        assert not state.has_more
        assert state.cursor is None

    def test_empty_response_has_column_schema(self, servicenow_env):
        """Empty response should still have correct column names."""
        from data_assets.assets.servicenow import ServiceNowIncidents

        df, _ = ServiceNowIncidents().parse_response({"result": []})
        assert "sys_id" in df.columns
        assert "number" in df.columns


# ---------------------------------------------------------------------------
# ServiceNow Changes
# ---------------------------------------------------------------------------


class TestServiceNowChanges:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChanges

        assert ServiceNowChanges().table_name == "change_request"

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChanges

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
        from data_assets.assets.servicenow import ServiceNowChanges
        from data_assets.assets.servicenow import ServiceNowIncidents

        assert issubclass(ServiceNowIncidents, ServiceNowTableAsset)
        assert issubclass(ServiceNowChanges, ServiceNowTableAsset)

    def test_keyset_pagination_config(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowIncidents

        asset = ServiceNowIncidents()
        assert asset.pagination_config.strategy == "keyset"
        assert asset.pagination_config.page_size == 1000
        assert asset.date_column == "sys_updated_on"


class TestServiceNowRegistration:
    """Verify all ServiceNow assets are discoverable after consolidation into tables.py."""

    EXPECTED_ASSETS = [
        "servicenow_incidents",
        "servicenow_changes",
        "servicenow_change_tasks",
        "servicenow_problems",
        "servicenow_users",
        "servicenow_user_groups",
        "servicenow_departments",
        "servicenow_locations",
        "servicenow_cmdb_cis",
        "servicenow_hardware_assets",
        "servicenow_catalog_items",
        "servicenow_catalog_requests",
        "servicenow_choices",
    ]

    def test_all_assets_registered(self, servicenow_env):
        from data_assets.core.registry import all_assets, discover

        discover()
        registered = set(all_assets().keys())
        for name in self.EXPECTED_ASSETS:
            assert name in registered, f"ServiceNow asset '{name}' missing from registry"


# ---------------------------------------------------------------------------
# pysnc client creation
# ---------------------------------------------------------------------------


class TestPysncClientCreation:
    def test_basic_auth(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowIncidents

        with patch("pysnc.ServiceNowClient") as mock_cls:
            ServiceNowIncidents()._create_pysnc_client()
            mock_cls.assert_called_once_with(
                "https://dev.service-now.com", ("admin", "pass"),
            )

    def test_oauth2_when_all_creds_set(self, servicenow_env, monkeypatch):
        from data_assets.assets.servicenow import ServiceNowIncidents

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
        from data_assets.assets.servicenow import ServiceNowIncidents

        monkeypatch.delenv("SERVICENOW_INSTANCE", raising=False)
        with pytest.raises(RuntimeError, match="SERVICENOW_INSTANCE"):
            ServiceNowIncidents()._create_pysnc_client()

    def test_missing_credentials_raises(self, monkeypatch):
        from data_assets.assets.servicenow import ServiceNowIncidents

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
        from data_assets.assets.servicenow import ServiceNowIncidents

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
        from data_assets.assets.servicenow import ServiceNowIncidents

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
        from data_assets.assets.servicenow import ServiceNowIncidents

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

    def test_extract_max_pages_stops_after_n_batches(self, servicenow_env):
        """max_pages=1 in context.params stops extraction after first full batch."""
        from data_assets.assets.servicenow import ServiceNowIncidents

        batch_size = 1000
        # Build 2 full batches + 1 partial — only the first should be written
        records = [
            {"sys_id": f"id{i}", "number": f"INC{i:04d}", "state": "1",
             "sys_updated_on": "2025-12-01T10:00:00Z"}
            for i in range(batch_size * 2 + 5)
        ]
        mock_gr = self._make_mock_gr(records)
        mock_client = MagicMock()
        mock_client.GlideRecord.return_value = mock_gr

        asset = ServiceNowIncidents()
        ctx = make_ctx(params={"max_pages": 1})

        write_calls = []

        def capture_write(engine, table, df):
            write_calls.append(len(df))
            return len(df)

        with (
            patch.object(asset, "_create_pysnc_client", return_value=mock_client),
            patch(
                "data_assets.assets.servicenow.base.write_to_temp",
                side_effect=capture_write,
            ),
        ):
            rows = asset.extract(MagicMock(), "temp_incidents", ctx)

        # Only 1 full batch written; break leaves batch empty so no partial flush
        assert write_calls[0] == batch_size
        assert len(write_calls) == 1
        assert rows == batch_size


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
# Table assets — parametrized (add new table assets here)
# ---------------------------------------------------------------------------

# Each entry: (class_name, table_name, fixture_file, expected_rows, field_checks)
_TABLE_ASSET_SPECS = [
    ("ServiceNowUsers", "sys_user", "users.json", 2, {"user_name": "john.doe"}),
    ("ServiceNowUserGroups", "sys_user_group", "user_groups.json", 2, {"name": "Network Team"}),
    ("ServiceNowLocations", "cmn_location", "locations.json", 2, {"city": "New York"}),
    ("ServiceNowDepartments", "cmn_department", "departments.json", 1, {"name": "Engineering"}),
    ("ServiceNowCmdbCIs", "cmdb_ci", "cmdb_cis.json", 2, {"name": "web-server-01", "sys_class_name": "cmdb_ci_server"}),
    ("ServiceNowHardwareAssets", "alm_hardware", "hardware_assets.json", 1, {"display_name": "Dell PowerEdge R740"}),
    ("ServiceNowProblems", "problem", "problems.json", 2, {"number": "PRB0010001"}),
    ("ServiceNowChangeTasks", "change_task", "change_tasks.json", 1, {"number": "CTASK0010001", "change_request": "chg001abc"}),
    ("ServiceNowCatalogRequests", "sc_request", "catalog_requests.json", 1, {"number": "REQ0010001"}),
    ("ServiceNowCatalogItems", "sc_req_item", "catalog_items.json", 1, {"number": "RITM0010001", "quantity": "1"}),
]


def _get_sn_cls(cls_name: str):
    """Import a ServiceNow asset class by name."""
    import data_assets.assets.servicenow as mod

    return getattr(mod, cls_name)


@pytest.mark.parametrize(
    "cls_name,table,fixture,rows,checks",
    _TABLE_ASSET_SPECS,
    ids=[s[0] for s in _TABLE_ASSET_SPECS],
)
class TestServiceNowTableAssets:
    def test_table_name(self, cls_name, table, fixture, rows, checks, servicenow_env):
        assert _get_sn_cls(cls_name)().table_name == table

    def test_primary_key(self, cls_name, table, fixture, rows, checks, servicenow_env):
        assert _get_sn_cls(cls_name)().primary_key == ["sys_id"]

    def test_parse_response(self, cls_name, table, fixture, rows, checks, servicenow_env):
        data = json.loads((FIXTURES / fixture).read_text())
        df, state = _get_sn_cls(cls_name)().parse_response(data)
        assert len(df) == rows
        for field, value in checks.items():
            assert df.iloc[0][field] == value
        assert not state.has_more

    def test_build_request_url(self, cls_name, table, fixture, rows, checks, servicenow_env):
        spec = _get_sn_cls(cls_name)().build_request(make_ctx())
        assert spec.url.endswith(f"/api/now/table/{table}")


# ---------------------------------------------------------------------------
# New assets: Choices (special — FULL_REPLACE, no incremental)
# ---------------------------------------------------------------------------


class TestServiceNowChoices:
    def test_table_name(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChoices

        assert ServiceNowChoices().table_name == "sys_choice"

    def test_primary_key(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChoices

        assert ServiceNowChoices().primary_key == ["sys_id"]

    def test_load_strategy(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChoices

        assert ServiceNowChoices().load_strategy == LoadStrategy.FULL_REPLACE

    def test_default_run_mode(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChoices

        assert ServiceNowChoices().default_run_mode == RunMode.FULL

    def test_date_column_none(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChoices

        assert ServiceNowChoices().date_column is None

    def test_parse_response(self, servicenow_env):
        from data_assets.assets.servicenow import ServiceNowChoices

        data = json.loads((FIXTURES / "choices.json").read_text())
        df, state = ServiceNowChoices().parse_response(data)
        assert len(df) == 3
        assert df.iloc[0]["label"] == "New"
        assert df.iloc[0]["name"] == "incident"
        assert df.iloc[0]["element"] == "state"
        assert not state.has_more

    def test_build_request_no_date_filter(self, servicenow_env):
        """FULL mode should not add a date filter to the query."""
        from data_assets.assets.servicenow import ServiceNowChoices

        spec = ServiceNowChoices().build_request(make_ctx())
        assert "sysparm_query" not in spec.params
