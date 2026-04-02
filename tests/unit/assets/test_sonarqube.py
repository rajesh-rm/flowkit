"""Unit tests for SonarQube assets: projects (RestAsset), issues (APIAsset), measures."""

from __future__ import annotations

import json
from pathlib import Path

from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "sonarqube"


# ---------------------------------------------------------------------------
# SonarQubeProjects (RestAsset — declarative, /api/components/search)
# ---------------------------------------------------------------------------


class TestSonarQubeProjects:
    def test_is_rest_asset(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects
        from data_assets.core.rest_asset import RestAsset

        assert issubclass(SonarQubeProjects, RestAsset)
        asset = SonarQubeProjects()
        assert asset.endpoint == "/api/components/search"
        assert asset.response_path == "components"

    def test_build_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        spec = SonarQubeProjects().build_request(make_ctx())
        assert spec.url == "https://sonar.test/api/components/search"
        assert spec.params["ps"] == 100
        assert spec.params["p"] == 1
        assert spec.params["qualifiers"] == "TRK"

    def test_build_request_with_checkpoint(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        spec = SonarQubeProjects().build_request(
            make_ctx(), checkpoint={"next_page": 3}
        )
        assert spec.params["p"] == 3
        assert spec.params["qualifiers"] == "TRK"

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        data = json.loads((FIXTURES / "projects_page1.json").read_text())
        df, state = SonarQubeProjects().parse_response(data)
        assert len(df) == 3
        assert "key" in df.columns
        assert "name" in df.columns
        assert "qualifier" in df.columns
        assert not state.has_more
        assert state.total_records == 3

    def test_primary_key_is_key(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        assert SonarQubeProjects().primary_key == ["key"]


# ---------------------------------------------------------------------------
# SonarQubeIssues (APIAsset — custom, entity-parallel, /api/issues/search)
# ---------------------------------------------------------------------------


class TestSonarQubeIssues:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        spec = SonarQubeIssues().build_entity_request("proj-alpha", make_ctx())
        assert spec.url == "https://sonar.test/api/issues/search"
        assert spec.params["componentKeys"] == "proj-alpha"
        assert spec.params["s"] == "UPDATE_DATE"
        assert spec.params["asc"] == "true"

    def test_tracks_watermark_on_update_date(self, sonarqube_env):
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        assert SonarQubeIssues().date_column == "update_date"

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        data = json.loads((FIXTURES / "issues_proj_alpha.json").read_text())
        df, state = SonarQubeIssues().parse_response(data)
        assert len(df) == 2
        assert "creation_date" in df.columns
        assert "update_date" in df.columns
        assert not state.has_more

    def test_parse_response_renames_date_fields(self, sonarqube_env):
        """API returns creationDate/updateDate, asset renames to snake_case."""
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        data = json.loads((FIXTURES / "issues_proj_alpha.json").read_text())
        df, _ = SonarQubeIssues().parse_response(data)
        assert "creationDate" not in df.columns
        assert "updateDate" not in df.columns
        assert "creation_date" in df.columns
        assert "update_date" in df.columns

    def test_build_request_delegates_to_entity(self, sonarqube_env):
        """build_request default delegates to build_entity_request for entity-parallel."""
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        spec = SonarQubeIssues().build_request(make_ctx())
        assert "componentKeys" in spec.params

    def test_pagination_state(self, sonarqube_env):
        """Verify pagination math from real-style response."""
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        data = {
            "paging": {"pageIndex": 1, "pageSize": 100, "total": 250},
            "issues": [{"key": f"issue-{i}", "rule": "r", "severity": "MAJOR",
                        "component": "c", "project": "p", "status": "OPEN",
                        "type": "BUG", "creationDate": "2025-01-01T00:00:00+0000",
                        "updateDate": "2025-01-01T00:00:00+0000"} for i in range(100)],
        }
        df, state = SonarQubeIssues().parse_response(data)
        assert state.has_more is True
        assert state.total_pages == 3
        assert state.next_page == 2
        assert state.total_records == 250


# ---------------------------------------------------------------------------
# SonarQubeMeasures (APIAsset — entity-parallel, /api/measures/component)
# ---------------------------------------------------------------------------


class TestSonarQubeMeasures:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        spec = SonarQubeMeasures().build_entity_request("proj-alpha", make_ctx())
        assert spec.url == "https://sonar.test/api/measures/component"
        assert spec.params["component"] == "proj-alpha"
        assert "ncloc" in spec.params["metricKeys"]
        assert "bugs" in spec.params["metricKeys"]

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        data = json.loads((FIXTURES / "measures_proj_alpha.json").read_text())
        df, state = SonarQubeMeasures().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["project_key"] == "proj-alpha"
        assert df.iloc[0]["ncloc"] == "12500"
        assert df.iloc[0]["bugs"] == "3"
        assert df.iloc[0]["coverage"] == "87.5"
        assert state.has_more is False

    def test_parse_empty_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        df, state = SonarQubeMeasures().parse_response({"component": {}})
        assert len(df) == 0
        assert state.has_more is False

    def test_parent_asset(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        assert SonarQubeMeasures().parent_asset_name == "sonarqube_projects"

    def test_primary_key(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        assert SonarQubeMeasures().primary_key == ["project_key"]
