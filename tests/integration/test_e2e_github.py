"""End-to-end tests for GitHub assets with mocked API and real Postgres.

Tests cover two organizations as specified.
"""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pandas as pd
import pytest
import respx

from data_assets.core.registry import _registry
from data_assets.extract.token_manager import TokenManager


def _clear_registry():
    _registry.clear()


def _register_github_assets():
    pass


GH_API = "https://api.github.com"


@pytest.mark.integration
@respx.mock
def test_github_repos_two_orgs(clean_db, monkeypatch, load_fixture):
    """Extract repos from two orgs, validate both land in the same table."""
    _clear_registry()
    monkeypatch.setenv("GITHUB_ORGS", "org-one,org-two")
    monkeypatch.setenv("GITHUB_APP_ID", "12345")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "fake-key")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "67890")

    _register_github_assets()

    org1_repos = load_fixture("github/repos_org1.json")
    org2_repos = load_fixture("github/repos_org2.json")

    # Mock org1 repos (2 repos, then empty page)
    respx.get(f"{GH_API}/orgs/org-one/repos").mock(
        side_effect=[
            httpx.Response(200, json=org1_repos),
            httpx.Response(200, json=[]),
        ]
    )

    # Mock org2 repos (1 repo, then empty page)
    respx.get(f"{GH_API}/orgs/org-two/repos").mock(
        side_effect=[
            httpx.Response(200, json=org2_repos),
            httpx.Response(200, json=[]),
        ]
    )

    # Mock GitHub App token generation (skip JWT by patching token manager)
    from data_assets.extract.token_manager import GitHubAppTokenManager

    with patch.object(GitHubAppTokenManager, "__init__", lambda self: TokenManager.__init__(self)):
        with patch.object(GitHubAppTokenManager, "get_token", return_value="fake-token"):
            with patch.object(
                GitHubAppTokenManager, "get_auth_header",
                return_value={"Authorization": "Bearer fake-token"},
            ):
                with patch("data_assets.runner.get_engine", return_value=clean_db):
                    with patch("data_assets.db.engine.get_engine", return_value=clean_db):
                        from data_assets.runner import run_asset
                        result = run_asset("github_repos", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_loaded"] == 3  # 2 from org-one + 1 from org-two

    df = pd.read_sql("SELECT * FROM raw.github_repos ORDER BY id", clean_db)
    assert len(df) == 3
    assert set(df["owner_login"]) == {"org-one", "org-two"}
    assert "org-one/service-api" in df["full_name"].values
    assert "org-two/data-pipeline" in df["full_name"].values




@pytest.mark.integration
@respx.mock
def test_github_pull_requests_entity_parallel(clean_db, monkeypatch, load_fixture):
    """Extract PRs per repo using entity-parallel mode."""
    _clear_registry()
    monkeypatch.setenv("GITHUB_ORGS", "org-one")
    monkeypatch.setenv("GITHUB_APP_ID", "12345")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "fake-key")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "67890")

    _register_github_assets()

    # Seed the parent table
    repos_df = pd.DataFrame([
        {"id": 100001, "full_name": "org-one/service-api", "name": "service-api",
         "owner_login": "org-one", "private": "false", "description": "API",
         "language": "Python", "default_branch": "main",
         "created_at": "2024-01-15", "updated_at": "2025-12-01",
         "pushed_at": "2025-12-01", "archived": "false",
         "html_url": "https://github.com/org-one/service-api"},
    ])
    repos_df.to_sql("github_repos", clean_db, schema="raw", if_exists="replace", index=False)

    prs_data = load_fixture("github/pull_requests.json")

    respx.get(f"{GH_API}/repos/org-one/service-api/pulls").mock(
        return_value=httpx.Response(200, json=prs_data)
    )

    from data_assets.extract.token_manager import GitHubAppTokenManager

    with patch.object(GitHubAppTokenManager, "__init__", lambda self: TokenManager.__init__(self)):
        with patch.object(GitHubAppTokenManager, "get_token", return_value="fake-token"):
            with patch.object(
                GitHubAppTokenManager, "get_auth_header",
                return_value={"Authorization": "Bearer fake-token"},
            ):
                with patch("data_assets.runner.get_engine", return_value=clean_db):
                    with patch("data_assets.db.engine.get_engine", return_value=clean_db):
                        from data_assets.runner import run_asset
                        result = run_asset("github_pull_requests", run_mode="full")

    assert result["status"] == "success"
    assert result["rows_loaded"] == 2

    df = pd.read_sql("SELECT * FROM raw.github_pull_requests ORDER BY id", clean_db)
    assert len(df) == 2
    assert 42 in df["number"].values
    assert "dev-alice" in df["user_login"].values
