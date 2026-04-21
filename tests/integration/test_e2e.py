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
        assert result["rows_loaded"] == 5


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
        assert result["rows_loaded"] == 5

        df = pd.read_sql("SELECT * FROM raw.servicenow_incidents ORDER BY number", run_engine)
        assert "INC0010001" in df["number"].values
        assert "INC0010005" in df["number"].values

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
        assert result["rows_loaded"] == 5

        df = pd.read_sql("SELECT * FROM raw.github_repos ORDER BY id", run_engine)
        assert len(df) == 5
        assert set(df["owner_login"]) == {"org-one"}

    @respx.mock
    def test_pull_requests_entity_parallel(self, run_engine, monkeypatch, load_fixture):
        monkeypatch.setenv("GITHUB_ORGS", "org-one")
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_PRIVATE_KEY", "fake-key")
        monkeypatch.setenv("GITHUB_INSTALLATION_ID", "67890")

        seed_table(run_engine, "raw", "github_repos", [
            {"id": 100001, "full_name": "org-one/service-api", "name": "service-api",
             "owner_login": "org-one", "private": False, "description": "API",
             "language": "Python", "default_branch": "main",
             "created_at": "2024-01-15", "updated_at": "2025-12-01",
             "pushed_at": "2025-12-01", "archived": False,
             "html_url": "https://github.com/org-one/service-api"},
        ])

        respx.get(f"{GH_API}/repos/org-one/service-api/pulls").mock(
            return_value=httpx.Response(200, json=load_fixture("github/pull_requests.json"))
        )

        with stub_token_manager(GitHubAppTokenManager):
            from data_assets.runner import run_asset
            result = run_asset("github_pull_requests", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 5

        df = pd.read_sql("SELECT * FROM raw.github_pull_requests ORDER BY id", run_engine)
        assert 42 in df["number"].values
        assert "dev-alice" in df["user_login"].values

    @respx.mock
    def test_deployments_entity_parallel_graphql(
        self, run_engine, monkeypatch, load_fixture,
    ):
        """End-to-end the GraphQL deployments asset: POST /graphql, two pages, UPSERT."""
        monkeypatch.setenv("GITHUB_ORGS", "orgName")
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_PRIVATE_KEY", "fake-key")
        monkeypatch.setenv("GITHUB_INSTALLATION_ID", "67890")

        seed_table(run_engine, "raw", "github_repos", [
            {"id": 100001, "full_name": "orgName/devops-tooling",
             "name": "devops-tooling", "owner_login": "orgName",
             "private": False, "description": "tooling",
             "language": "Go", "default_branch": "main",
             "created_at": "2024-01-15", "updated_at": "2025-12-01",
             "pushed_at": "2025-12-01", "archived": False,
             "html_url": "https://github.com/orgName/devops-tooling"},
        ])

        respx.post(f"{GH_API}/graphql").mock(side_effect=[
            httpx.Response(200, json=load_fixture("github/deployments_graphql_page1.json")),
            httpx.Response(200, json=load_fixture("github/deployments_graphql_page2.json")),
        ])

        with stub_token_manager(GitHubAppTokenManager):
            from data_assets.runner import run_asset
            result = run_asset("github_deployments", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 12  # 10 from page1 + 2 from page2

        df = pd.read_sql(
            'SELECT * FROM raw.github_deployments ORDER BY deployment_id DESC',
            run_engine,
        )
        # Injected columns populated for every row
        assert (df["organization"] == "orgName").all()
        assert (df["repo_name"] == "devops-tooling").all()
        assert (df["org_repo_key"] == "orgName/devops-tooling").all()
        # Composite PK present
        assert len(df) == 12
        assert len(df.drop_duplicates(["deployment_id", "organization"])) == 12
        # source_url computed by transform()
        first = df.iloc[0]
        assert first["source_url"] == (
            f"https://github.com/orgName/devops-tooling/deployments/{first['deployment_id']}"
        )
        # Null-parent rows survived (fixture row 2404802687 has creator/latestStatus null)
        null_creator = df[df["deployment_id"] == 2404802687].iloc[0]
        assert pd.isna(null_creator["creator_login"])
        assert pd.isna(null_creator["latest_status"])

        # Run history + lock release
        history = pd.read_sql(
            "SELECT status FROM data_ops.run_history "
            "WHERE asset_name = 'github_deployments'",
            run_engine,
        )
        assert history.iloc[0]["status"] == "success"
        locks = pd.read_sql(
            "SELECT * FROM data_ops.run_locks "
            "WHERE asset_name = 'github_deployments'",
            run_engine,
        )
        assert len(locks) == 0


# ---------------------------------------------------------------------------
# Missing-key validation + null-rate warning
# ---------------------------------------------------------------------------


def _components_response(components: list[dict]) -> dict:
    return {
        "paging": {"pageIndex": 1, "pageSize": 100, "total": len(components)},
        "components": components,
    }


@pytest.mark.integration
class TestMissingKeyValidation:
    """End-to-end coverage for the missing-key block and null-rate warning."""

    @respx.mock
    def test_missing_required_key_blocks_promotion(
        self, run_engine, monkeypatch,
    ):
        """An API response missing a required key fails the run and drops the temp."""
        monkeypatch.setenv("SONARQUBE_URL", SONARQUBE_URL)
        monkeypatch.setenv("SONARQUBE_TOKEN", "fake-token")

        # Second component missing 'name' — required because 'name' is a
        # non-PK, non-index column of sonarqube_projects.
        response = _components_response([
            {"key": "ok", "name": "Ok", "qualifier": "TRK"},
            {"key": "broken", "qualifier": "TRK"},
        ])
        respx.get(f"{SONARQUBE_URL}/api/components/search").mock(
            return_value=httpx.Response(200, json=response)
        )

        from data_assets.runner import run_asset
        with pytest.raises(ValueError, match="is absent from response"):
            run_asset("sonarqube_projects", run_mode="full")

        # Target table untouched: either doesn't exist or is empty.
        rows = pd.read_sql(
            "SELECT to_regclass('raw.sonarqube_projects') AS t", run_engine,
        ).iloc[0]["t"]
        if rows is not None:
            df = pd.read_sql("SELECT * FROM raw.sonarqube_projects", run_engine)
            assert len(df) == 0

        # Run recorded as failed with the missing-key reason.
        history = pd.read_sql(
            "SELECT status, error_message FROM data_ops.run_history "
            "WHERE asset_name = 'sonarqube_projects'",
            run_engine,
        )
        assert history.iloc[0]["status"] == "failed"
        assert "is absent from response" in history.iloc[0]["error_message"]

    @respx.mock
    def test_missing_optional_key_succeeds(
        self, run_engine, monkeypatch,
    ):
        """An API response missing an OPTIONAL column key still succeeds."""
        monkeypatch.setenv("SONARQUBE_URL", SONARQUBE_URL)
        monkeypatch.setenv("SONARQUBE_TOKEN", "fake-token")

        # sonarqube_issues declares 'line' as optional; file-level issues
        # omit that key entirely.
        seed_table(run_engine, "raw", "sonarqube_projects", [
            {"key": "proj-a", "name": "A", "qualifier": "TRK"},
        ])

        issues_response = {
            "paging": {"pageIndex": 1, "pageSize": 100, "total": 1},
            "issues": [
                {
                    "key": "issue-1",
                    "rule": "rule",
                    "severity": "MAJOR",
                    "component": "proj-a:file.py",
                    "project": "proj-a",
                    # 'line' key omitted — optional
                    "message": "file-level issue",
                    "status": "OPEN",
                    "type": "BUG",
                    "creationDate": "2025-01-01T00:00:00+0000",
                    "updateDate": "2025-01-01T00:00:00+0000",
                },
            ],
        }
        respx.get(f"{SONARQUBE_URL}/api/issues/search").mock(
            return_value=httpx.Response(200, json=issues_response)
        )

        from data_assets.runner import run_asset
        result = run_asset("sonarqube_issues", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 1

        df = pd.read_sql(
            "SELECT line FROM raw.sonarqube_issues WHERE key = 'issue-1'",
            run_engine,
        )
        # Absent key lands as NULL in the DB
        assert pd.isna(df.iloc[0]["line"])

    @respx.mock
    def test_high_null_rate_emits_warning_but_succeeds(
        self, run_engine, monkeypatch,
    ):
        """A column with lots of NULL values should trigger a warning, not a failure."""
        monkeypatch.setenv("SONARQUBE_URL", SONARQUBE_URL)
        monkeypatch.setenv("SONARQUBE_TOKEN", "fake-token")

        # Every component has all keys present (no MissingKeyError), but
        # 'name' is null for most rows — trips the 2% null-rate warning.
        components = [{"key": f"p{i}", "name": None, "qualifier": "TRK"} for i in range(49)]
        components.append({"key": "p-named", "name": "Named", "qualifier": "TRK"})
        respx.get(f"{SONARQUBE_URL}/api/components/search").mock(
            return_value=httpx.Response(200, json=_components_response(components))
        )

        from data_assets.runner import run_asset
        result = run_asset("sonarqube_projects", run_mode="full")

        assert result["status"] == "success"
        assert result["rows_loaded"] == 50

        # Warning captured in run_history metadata (column name 'metadata',
        # mapped to Python attribute 'metadata_' to avoid the SQLA keyword clash)
        import json

        row = pd.read_sql(
            "SELECT metadata FROM data_ops.run_history "
            "WHERE asset_name = 'sonarqube_projects'",
            run_engine,
        ).iloc[0]["metadata"]
        meta = row if isinstance(row, dict) else json.loads(row)
        warnings = meta.get("warnings", [])
        assert any("High null rate" in w for w in warnings)
