"""End-to-end tests for Jira assets with mocked API and real Postgres."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pandas as pd
import pytest
import respx

from data_assets.core.registry import _registry


def _clear_registry():
    _registry.clear()


def _register_jira_assets():
    from data_assets.assets.jira.projects import JiraProjects
    from data_assets.assets.jira.issues import JiraIssues


JIRA_URL = "https://mysite.atlassian.net"


@pytest.mark.integration
@respx.mock
def test_jira_projects_full_run(clean_db, monkeypatch, load_fixture):
    """Full lifecycle: extract Jira projects."""
    _clear_registry()
    monkeypatch.setenv("JIRA_URL", JIRA_URL)
    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "fake-jira-token")

    _register_jira_assets()

    projects_data = load_fixture("jira/projects.json")

    respx.get(f"{JIRA_URL}/rest/api/3/project/search").mock(
        return_value=httpx.Response(200, json=projects_data)
    )

    with patch("data_assets.runner.get_engine", return_value=clean_db):
        with patch("data_assets.db.engine.get_engine", return_value=clean_db):
            from data_assets.runner import run_asset
            result = run_asset("jira_projects", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_loaded"] == 2

    df = pd.read_sql("SELECT * FROM raw.jira_projects ORDER BY key", clean_db)
    assert len(df) == 2
    assert list(df["key"]) == ["ENG", "OPS"]


@pytest.mark.integration
@respx.mock
def test_jira_issues_entity_parallel(clean_db, monkeypatch, load_fixture):
    """Extract issues per project using entity-parallel mode."""
    _clear_registry()
    monkeypatch.setenv("JIRA_URL", JIRA_URL)
    monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "fake-jira-token")

    _register_jira_assets()

    # Seed parent table (jira_projects)
    projects_df = pd.DataFrame([
        {"key": "ENG", "id": "10001", "name": "Engineering",
         "project_type_key": "software", "style": "next-gen", "is_private": "false"},
    ])
    projects_df.to_sql("jira_projects", clean_db, schema="raw", if_exists="replace", index=False)

    issues_data = load_fixture("jira/issues_eng.json")

    respx.get(f"{JIRA_URL}/rest/api/3/search").mock(
        return_value=httpx.Response(200, json=issues_data)
    )

    with patch("data_assets.runner.get_engine", return_value=clean_db):
        with patch("data_assets.db.engine.get_engine", return_value=clean_db):
            from data_assets.runner import run_asset
            result = run_asset("jira_issues", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_loaded"] == 2

    df = pd.read_sql("SELECT * FROM raw.jira_issues ORDER BY key", clean_db)
    assert len(df) == 2
    assert "ENG-101" in df["key"].values
    assert "ENG-102" in df["key"].values
    # Verify nested field extraction
    assert "Alice Chen" in df["assignee"].values
    assert "backend,api" in df["labels"].values or "api,backend" in df["labels"].values
