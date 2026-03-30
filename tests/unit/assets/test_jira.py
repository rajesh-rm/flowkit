"""Unit tests for Jira asset build_request/parse_response."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "jira"


def _ctx(**kwargs):
    return RunContext(
        run_id=uuid.uuid4(), mode=RunMode.FULL, asset_name="test", **kwargs
    )


def test_projects_build_request(monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://jira.test")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    from data_assets.assets.jira.projects import JiraProjects

    asset = JiraProjects()
    spec = asset.build_request(_ctx(), checkpoint=None)
    assert spec.url == "https://jira.test/rest/api/3/project/search"
    assert spec.params["startAt"] == 0
    assert spec.params["maxResults"] == 50


def test_projects_parse_response(monkeypatch):
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    from data_assets.assets.jira.projects import JiraProjects

    data = json.loads((FIXTURES / "projects.json").read_text())
    asset = JiraProjects()
    df, state = asset.parse_response(data)

    assert len(df) == 2
    assert list(df["key"]) == ["ENG", "OPS"]
    assert not state.has_more  # isLast=True


def test_issues_build_entity_request(monkeypatch):
    monkeypatch.setenv("JIRA_URL", "https://jira.test")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    from data_assets.assets.jira.issues import JiraIssues

    asset = JiraIssues()
    spec = asset.build_entity_request("ENG", _ctx(), checkpoint=None)
    assert spec.url == "https://jira.test/rest/api/3/search"
    assert 'project = "ENG"' in spec.params["jql"]


def test_issues_parse_response(monkeypatch):
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")
    from data_assets.assets.jira.issues import JiraIssues

    data = json.loads((FIXTURES / "issues_eng.json").read_text())
    asset = JiraIssues()
    df, state = asset.parse_response(data)

    assert len(df) == 2
    assert "ENG-101" in df["key"].values
    assert "Alice Chen" in df["assignee"].values
    assert not state.has_more  # 2 issues, total=2
    assert state.next_offset == 2
