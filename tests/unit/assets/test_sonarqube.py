"""Unit tests for SonarQube assets: projects (RestAsset) and issues (APIAsset)."""

from __future__ import annotations

import json
from pathlib import Path

from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "sonarqube"


# ---------------------------------------------------------------------------
# SonarQubeProjects (RestAsset — declarative)
# ---------------------------------------------------------------------------


class TestSonarQubeProjects:
    def test_is_rest_asset(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects
        from data_assets.core.rest_asset import RestAsset

        assert issubclass(SonarQubeProjects, RestAsset)
        asset = SonarQubeProjects()
        assert asset.endpoint == "/api/projects/search"
        assert asset.response_path == "components"

    def test_build_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        spec = SonarQubeProjects().build_request(make_ctx())
        assert spec.url == "https://sonar.test/api/projects/search"
        assert spec.params["ps"] == 100
        assert spec.params["p"] == 1

    def test_build_request_with_checkpoint(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        spec = SonarQubeProjects().build_request(
            make_ctx(), checkpoint={"next_page": 3}
        )
        assert spec.params["p"] == 3

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        data = json.loads((FIXTURES / "projects_page1.json").read_text())
        df, state = SonarQubeProjects().parse_response(data)
        assert len(df) == 3
        assert "key" in df.columns
        assert "last_analysis_date" in df.columns  # field_map applied
        assert not state.has_more
        assert state.total_records == 3


# ---------------------------------------------------------------------------
# SonarQubeIssues (APIAsset — custom, entity-parallel)
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

    def test_build_request_delegates_to_entity(self, sonarqube_env):
        """build_request (abstract method) delegates to build_entity_request."""
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        spec = SonarQubeIssues().build_request(make_ctx())
        assert "componentKeys" in spec.params  # delegates, not a different code path
