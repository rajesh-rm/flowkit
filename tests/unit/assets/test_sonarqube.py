"""Unit tests for SonarQube asset build_request/parse_response."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "sonarqube"


def _ctx(**kwargs):
    return RunContext(
        run_id=uuid.uuid4(), mode=RunMode.FULL, asset_name="test", **kwargs
    )


def test_projects_build_request(monkeypatch):
    monkeypatch.setenv("SONARQUBE_URL", "https://sonar.test")
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")
    from data_assets.assets.sonarqube.projects import SonarQubeProjects

    asset = SonarQubeProjects()
    spec = asset.build_request(_ctx())
    assert spec.url == "https://sonar.test/api/projects/search"
    assert spec.params["ps"] == 100
    assert spec.params["p"] == 1


def test_projects_build_request_with_checkpoint(monkeypatch):
    monkeypatch.setenv("SONARQUBE_URL", "https://sonar.test")
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")
    from data_assets.assets.sonarqube.projects import SonarQubeProjects

    asset = SonarQubeProjects()
    spec = asset.build_request(_ctx(), checkpoint={"page": 3})
    assert spec.params["p"] == 3


def test_projects_parse_response(monkeypatch):
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")
    from data_assets.assets.sonarqube.projects import SonarQubeProjects

    data = json.loads((FIXTURES / "projects_page1.json").read_text())
    asset = SonarQubeProjects()
    df, state = asset.parse_response(data)

    assert len(df) == 3
    assert "key" in df.columns
    assert list(df["key"]) == ["proj-alpha", "proj-beta", "proj-gamma"]
    assert not state.has_more  # 3 items, page_size=100 → single page
    assert state.total_records == 3


def test_issues_build_entity_request(monkeypatch):
    monkeypatch.setenv("SONARQUBE_URL", "https://sonar.test")
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")
    from data_assets.assets.sonarqube.issues import SonarQubeIssues

    asset = SonarQubeIssues()
    spec = asset.build_entity_request("proj-alpha", _ctx(), checkpoint=None)
    assert spec.url == "https://sonar.test/api/issues/search"
    assert spec.params["componentKeys"] == "proj-alpha"
    # Must sort by UPDATE_DATE ascending for reliable incremental
    assert spec.params["s"] == "UPDATE_DATE"
    assert spec.params["asc"] == "true"


def test_issues_tracks_watermark_on_update_date(monkeypatch):
    """date_column must be update_date (not creation_date) to catch all changes."""
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")
    from data_assets.assets.sonarqube.issues import SonarQubeIssues

    asset = SonarQubeIssues()
    assert asset.date_column == "update_date"


def test_issues_parse_response(monkeypatch):
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")
    from data_assets.assets.sonarqube.issues import SonarQubeIssues

    data = json.loads((FIXTURES / "issues_proj_alpha.json").read_text())
    asset = SonarQubeIssues()
    df, state = asset.parse_response(data)

    assert len(df) == 2
    assert "creation_date" in df.columns  # renamed from creationDate
    assert "update_date" in df.columns  # renamed from updateDate
    assert not state.has_more
