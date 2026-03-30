"""Unit tests for Jira assets: projects and issues."""

from __future__ import annotations

import json
from pathlib import Path

from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "jira"


# ---------------------------------------------------------------------------
# JiraProjects
# ---------------------------------------------------------------------------


class TestJiraProjectsBuildRequest:
    def test_basic(self, jira_env):
        from data_assets.assets.jira.projects import JiraProjects

        spec = JiraProjects().build_request(make_ctx(), checkpoint=None)
        assert spec.url == "https://jira.test/rest/api/3/project/search"
        assert spec.params["startAt"] == 0
        assert spec.params["maxResults"] == 50

    def test_with_offset_checkpoint(self, jira_env):
        from data_assets.assets.jira.projects import JiraProjects

        spec = JiraProjects().build_request(make_ctx(), checkpoint={"next_offset": 50})
        assert spec.params["startAt"] == 50


class TestJiraProjectsParseResponse:
    def test_happy_path(self, jira_env):
        from data_assets.assets.jira.projects import JiraProjects

        data = json.loads((FIXTURES / "projects.json").read_text())
        df, state = JiraProjects().parse_response(data)
        assert len(df) == 2
        assert list(df["key"]) == ["ENG", "OPS"]
        assert not state.has_more


# ---------------------------------------------------------------------------
# JiraIssues — JQL building
# ---------------------------------------------------------------------------


class TestJiraIssuesJQL:
    def test_with_project_and_date(self, jira_env):
        from data_assets.assets.jira.issues import JiraIssues

        jql = JiraIssues._build_jql(project_key="ENG", start_date="2025-01-01")
        assert 'project = "ENG"' in jql
        assert 'updated >= "2025-01-01"' in jql
        assert "ORDER BY updated ASC" in jql

    def test_project_only(self, jira_env):
        from data_assets.assets.jira.issues import JiraIssues

        jql = JiraIssues._build_jql(project_key="ENG")
        assert 'project = "ENG"' in jql
        assert "ORDER BY updated ASC" in jql

    def test_no_clauses(self):
        from data_assets.assets.jira.issues import JiraIssues

        jql = JiraIssues._build_jql()
        assert jql == "ORDER BY updated ASC"


class TestJiraIssuesBuildRequest:
    def test_entity_request(self, jira_env):
        from data_assets.assets.jira.issues import JiraIssues

        spec = JiraIssues().build_entity_request("ENG", make_ctx(), checkpoint=None)
        assert spec.url == "https://jira.test/rest/api/3/search"
        assert 'project = "ENG"' in spec.params["jql"]
        assert spec.params["startAt"] == 0


class TestJiraIssuesParseResponse:
    def test_happy_path(self, jira_env):
        from data_assets.assets.jira.issues import JiraIssues

        data = json.loads((FIXTURES / "issues_eng.json").read_text())
        df, state = JiraIssues().parse_response(data)
        assert len(df) == 2
        assert "ENG-101" in df["key"].values
        assert "Alice Chen" in df["assignee"].values
        assert not state.has_more
        assert state.next_offset == 2

    def test_tracks_watermark_on_updated(self, jira_env):
        from data_assets.assets.jira.issues import JiraIssues

        assert JiraIssues().date_column == "updated"
