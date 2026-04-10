"""End-to-end tests: full run_asset() lifecycle with mocked APIs and real Postgres.

Each test exercises the complete pipeline: lock → extract → validate → promote → finalize.
"""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pandas as pd
import pytest
import respx

from data_assets.extract.token_manager import GitHubAppTokenManager
from data_assets.load.loader import write_to_temp
from tests.integration.conftest import seed_table, stub_token_manager


# ---------------------------------------------------------------------------
# SonarQube
# ---------------------------------------------------------------------------


SONARQUBE_URL = "https://sonar.test.local"


@pytest.mark.integration
class TestSonarQubeE2E:
    @respx.mock
    def test_projects_full_run(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("SONARQUBE_URL", SONARQUBE_URL)
        monkeypatch.setenv("SONARQUBE_TOKEN", "fake-token")

        respx.get(f"{SONARQUBE_URL}/api/components/search").mock(
            return_value=httpx.Response(200, json=load_fixture("sonarqube/projects_page1.json"))
        )

        from data_assets.runner import run_asset
        result = run_asset("sonarqube_projects", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 3

        df = pd.read_sql('SELECT * FROM raw.sonarqube_projects ORDER BY "key"', run_engine)
        assert list(df["key"]) == ["proj-alpha", "proj-beta", "proj-gamma"]

        # Verify run history recorded and lock released
        history = pd.read_sql(
            "SELECT * FROM data_ops.run_history WHERE asset_name = 'sonarqube_projects'",
            run_engine,
        )
        assert len(history) == 1
        assert history.iloc[0]["status"] == "success"

        locks = pd.read_sql(
            "SELECT * FROM data_ops.run_locks WHERE asset_name = 'sonarqube_projects'",
            run_engine,
        )
        assert len(locks) == 0

    @respx.mock
    def test_issues_entity_parallel(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("SONARQUBE_URL", SONARQUBE_URL)
        monkeypatch.setenv("SONARQUBE_TOKEN", "fake-token")

        seed_table(run_engine, "raw", "sonarqube_projects", [
            {"key": "proj-alpha", "name": "Project Alpha", "qualifier": "TRK"},
        ])

        respx.get(f"{SONARQUBE_URL}/api/issues/search").mock(
            return_value=httpx.Response(200, json=load_fixture("sonarqube/issues_proj_alpha.json"))
        )

        from data_assets.runner import run_asset
        result = run_asset("sonarqube_issues", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 2


# ---------------------------------------------------------------------------
# ServiceNow
# ---------------------------------------------------------------------------


SNOW_URL = "https://dev12345.service-now.com"


def _mock_pysnc_extract(fixture_data):
    """Return a mock extract() that writes fixture data to temp table via pysnc bypass.

    ServiceNow assets use pysnc (requests-based), not httpx, so respx can't
    intercept their calls. Instead we mock extract() to write fixture records
    directly — the rest of the pipeline (validate, promote, finalize) runs for real.
    """
    records = fixture_data["result"]

    def _extract(self, engine, temp_table, context):
        column_names = [c.name for c in self.columns]
        df = pd.DataFrame(records)
        df = df[[c for c in column_names if c in df.columns]]
        return write_to_temp(engine, temp_table, df)

    return _extract


@pytest.mark.integration
class TestServiceNowE2E:
    def test_incidents_full_run(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("SERVICENOW_INSTANCE", SNOW_URL)
        monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
        monkeypatch.setenv("SERVICENOW_PASSWORD", "test-pass")

        fixture = load_fixture("servicenow/incidents.json")
        from data_assets.assets.servicenow.base import ServiceNowTableAsset

        with patch.object(ServiceNowTableAsset, "extract", _mock_pysnc_extract(fixture)):
            from data_assets.runner import run_asset
            result = run_asset("servicenow_incidents", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 2

        df = pd.read_sql("SELECT * FROM raw.servicenow_incidents ORDER BY number", run_engine)
        assert "INC0010001" in df["number"].values
        assert "INC0010002" in df["number"].values

    def test_changes_full_run(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("SERVICENOW_INSTANCE", SNOW_URL)
        monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
        monkeypatch.setenv("SERVICENOW_PASSWORD", "test-pass")

        fixture = load_fixture("servicenow/changes.json")
        from data_assets.assets.servicenow.base import ServiceNowTableAsset

        with patch.object(ServiceNowTableAsset, "extract", _mock_pysnc_extract(fixture)):
            from data_assets.runner import run_asset
            result = run_asset("servicenow_changes", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 1


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------


JIRA_URL = "https://mysite.atlassian.net"


@pytest.mark.integration
class TestJiraE2E:
    @respx.mock
    def test_projects_full_run(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("JIRA_URL", JIRA_URL)
        monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "fake-jira-token")

        respx.get(f"{JIRA_URL}/rest/api/3/project/search").mock(
            return_value=httpx.Response(200, json=load_fixture("jira/projects.json"))
        )

        from data_assets.runner import run_asset
        result = run_asset("jira_projects", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 2

        df = pd.read_sql("SELECT * FROM raw.jira_projects", run_engine)
        assert sorted(df["key"].tolist()) == ["ENG", "OPS"]

    @respx.mock
    def test_issues_entity_parallel(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("JIRA_URL", JIRA_URL)
        monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "fake-jira-token")

        seed_table(run_engine, "raw", "jira_projects", [
            {"key": "ENG", "id": "10001", "name": "Engineering",
             "project_type_key": "software", "style": "next-gen", "is_private": "false"},
        ])

        respx.get(f"{JIRA_URL}/rest/api/3/search").mock(
            return_value=httpx.Response(200, json=load_fixture("jira/issues_eng.json"))
        )

        from data_assets.runner import run_asset
        result = run_asset("jira_issues", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 2

        df = pd.read_sql("SELECT * FROM raw.jira_issues", run_engine)
        assert "ENG-101" in df["key"].values
        assert "Alice Chen" in df["assignee"].values


# ---------------------------------------------------------------------------
# GitHub (requires token manager stub — JWT signing needs real crypto)
# ---------------------------------------------------------------------------


GH_API = "https://api.github.com"


@pytest.mark.integration
class TestGitHubE2E:
    @respx.mock
    def test_repos_single_org(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("GITHUB_ORGS", "org-one")
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_PRIVATE_KEY", "fake-key")
        monkeypatch.setenv("GITHUB_INSTALLATION_ID", "67890")

        respx.get(f"{GH_API}/orgs/org-one/repos").mock(
            side_effect=[
                httpx.Response(200, json=load_fixture("github/repos_org1.json")),
                httpx.Response(200, json=[]),
            ]
        )

        with stub_token_manager(GitHubAppTokenManager):
            from data_assets.runner import run_asset
            result = run_asset("github_repos", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 2

        df = pd.read_sql("SELECT * FROM raw.github_repos ORDER BY id", run_engine)
        assert len(df) == 2
        assert set(df["owner_login"]) == {"org-one"}

    @respx.mock
    def test_pull_requests_entity_parallel(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("GITHUB_ORGS", "org-one")
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_PRIVATE_KEY", "fake-key")
        monkeypatch.setenv("GITHUB_INSTALLATION_ID", "67890")

        seed_table(run_engine, "raw", "github_repos", [
            {"id": 100001, "full_name": "org-one/service-api", "name": "service-api",
             "owner_login": "org-one", "private": "false", "description": "API",
             "language": "Python", "default_branch": "main",
             "created_at": "2024-01-15", "updated_at": "2025-12-01",
             "pushed_at": "2025-12-01", "archived": "false",
             "html_url": "https://github.com/org-one/service-api"},
        ])

        respx.get(f"{GH_API}/repos/org-one/service-api/pulls").mock(
            return_value=httpx.Response(200, json=load_fixture("github/pull_requests.json"))
        )

        with stub_token_manager(GitHubAppTokenManager):
            from data_assets.runner import run_asset
            result = run_asset("github_pull_requests", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 2

        df = pd.read_sql("SELECT * FROM raw.github_pull_requests ORDER BY id", run_engine)
        assert 42 in df["number"].values
        assert "dev-alice" in df["user_login"].values
