"""End-to-end tests for SonarQube assets with mocked API and real Postgres."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pandas as pd
import pytest
import respx

from data_assets.core.registry import _registry, register


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clear_registry():
    _registry.clear()


def _register_sonarqube_assets():
    """Import to trigger @register decorators."""
    from data_assets.assets.sonarqube.projects import SonarQubeProjects
    from data_assets.assets.sonarqube.issues import SonarQubeIssues


SONARQUBE_URL = "https://sonar.test.local"


# ---------------------------------------------------------------------------
# Projects: full extract with page-parallel
# ---------------------------------------------------------------------------

@pytest.mark.integration
@respx.mock
def test_sonarqube_projects_full_run(clean_db, monkeypatch, load_fixture):
    """Full lifecycle: extract SonarQube projects, validate, promote to raw."""
    _clear_registry()
    monkeypatch.setenv("SONARQUBE_URL", SONARQUBE_URL)
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake-token")

    _register_sonarqube_assets()

    projects_data = load_fixture("sonarqube/projects_page1.json")

    # Mock the API — single page (total=3, page_size=100 → 1 page)
    respx.get(f"{SONARQUBE_URL}/api/projects/search").mock(
        return_value=httpx.Response(200, json=projects_data)
    )

    # Patch get_engine to use our test engine
    with patch("data_assets.runner.get_engine", return_value=clean_db):
        with patch("data_assets.db.engine.get_engine", return_value=clean_db):
            from data_assets.runner import run_asset
            result = run_asset("sonarqube_projects", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_extracted"] == 3
    assert result["rows_loaded"] == 3

    # Verify data landed in raw.sonarqube_projects
    df = pd.read_sql('SELECT * FROM raw.sonarqube_projects ORDER BY "key"', clean_db)
    assert len(df) == 3
    assert list(df["key"]) == ["proj-alpha", "proj-beta", "proj-gamma"]

    # Verify run_history recorded
    history = pd.read_sql(
        "SELECT * FROM data_ops.run_history WHERE asset_name = 'sonarqube_projects'",
        clean_db,
    )
    assert len(history) == 1
    assert history.iloc[0]["status"] == "success"

    # Verify lock was released
    locks = pd.read_sql(
        "SELECT * FROM data_ops.run_locks WHERE asset_name = 'sonarqube_projects'",
        clean_db,
    )
    assert len(locks) == 0


# ---------------------------------------------------------------------------
# Issues: entity-parallel extract (per project)
# ---------------------------------------------------------------------------

@pytest.mark.integration
@respx.mock
def test_sonarqube_issues_entity_parallel(clean_db, monkeypatch, load_fixture):
    """Extract issues per project using entity-parallel mode."""
    _clear_registry()
    monkeypatch.setenv("SONARQUBE_URL", SONARQUBE_URL)
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake-token")

    _register_sonarqube_assets()

    # First, seed the parent table (sonarqube_projects)
    projects_df = pd.DataFrame([
        {"key": "proj-alpha", "name": "Project Alpha", "qualifier": "TRK",
         "visibility": "public", "last_analysis_date": "2025-12-01", "revision": "abc"},
    ])
    projects_df.to_sql(
        "sonarqube_projects", clean_db, schema="raw", if_exists="replace", index=False
    )

    issues_data = load_fixture("sonarqube/issues_proj_alpha.json")

    # Mock the issues API for proj-alpha
    respx.get(f"{SONARQUBE_URL}/api/issues/search").mock(
        return_value=httpx.Response(200, json=issues_data)
    )

    with patch("data_assets.runner.get_engine", return_value=clean_db):
        with patch("data_assets.db.engine.get_engine", return_value=clean_db):
            from data_assets.runner import run_asset
            result = run_asset("sonarqube_issues", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_loaded"] == 2

    df = pd.read_sql('SELECT * FROM raw.sonarqube_issues ORDER BY "key"', clean_db)
    assert len(df) == 2
    assert "AXyz-issue-001" in df["key"].values
