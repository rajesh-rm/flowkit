"""Unit tests for GitHub asset build_request/parse_response."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pandas as pd

from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "github"


def _ctx(**kwargs):
    return RunContext(
        run_id=uuid.uuid4(), mode=RunMode.FULL, asset_name="test", **kwargs
    )


def test_repos_build_request_first_org(monkeypatch):
    monkeypatch.setenv("GITHUB_ORGS", "org-one,org-two")
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from data_assets.assets.github.repos import GitHubRepos

    asset = GitHubRepos()
    spec = asset.build_request(_ctx())
    assert "/orgs/org-one/repos" in spec.url
    assert spec.params["page"] == 1


def test_repos_build_request_second_org_via_checkpoint(monkeypatch):
    monkeypatch.setenv("GITHUB_ORGS", "org-one,org-two")
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from data_assets.assets.github.repos import GitHubRepos

    asset = GitHubRepos()
    spec = asset.build_request(_ctx(), checkpoint={"org_idx": 1, "next_page": 1})
    assert "/orgs/org-two/repos" in spec.url


def test_repos_parse_response(monkeypatch):
    monkeypatch.setenv("GITHUB_ORGS", "org-one")
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from data_assets.assets.github.repos import GitHubRepos

    data = json.loads((FIXTURES / "repos_org1.json").read_text())
    asset = GitHubRepos()
    df, state = asset.parse_response(data)

    assert len(df) == 2
    assert "full_name" in df.columns
    assert "owner_login" in df.columns
    assert df.iloc[0]["owner_login"] == "org-one"
    assert not state.has_more  # 2 repos < page_size 100


def test_repos_parse_empty_response(monkeypatch):
    monkeypatch.setenv("GITHUB_ORGS", "org-one")
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from data_assets.assets.github.repos import GitHubRepos

    asset = GitHubRepos()
    df, state = asset.parse_response([])
    assert len(df) == 0
    assert not state.has_more


def test_pull_requests_build_entity_request(monkeypatch):
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from data_assets.assets.github.pull_requests import GitHubPullRequests

    asset = GitHubPullRequests()
    spec = asset.build_entity_request("org-one/service-api", _ctx())
    assert "/repos/org-one/service-api/pulls" in spec.url
    assert spec.params["state"] == "all"
    assert "since" not in spec.params  # since param removed (not supported by GitHub)


def test_pull_requests_parse_response(monkeypatch):
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from data_assets.assets.github.pull_requests import GitHubPullRequests

    data = json.loads((FIXTURES / "pull_requests.json").read_text())
    asset = GitHubPullRequests()
    df, state = asset.parse_response(data)

    assert len(df) == 2
    assert "user_login" in df.columns
    assert df.iloc[0]["user_login"] == "dev-alice"
    assert not state.has_more


def test_pull_requests_should_stop_in_forward_mode(monkeypatch):
    """In FORWARD mode, should_stop returns True when page is older than watermark."""
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from datetime import UTC, datetime

    from data_assets.assets.github.pull_requests import GitHubPullRequests

    asset = GitHubPullRequests()

    # Context with start_date = Dec 1 (watermark from last run)
    forward_ctx = _ctx(
        start_date=datetime(2025, 12, 1, tzinfo=UTC),
    )
    # Override mode to FORWARD
    from dataclasses import replace
    forward_ctx = replace(forward_ctx, mode=RunMode.FORWARD)

    # Page with PRs updated before the watermark → should stop
    old_df = pd.DataFrame({"updated_at": ["2025-11-28T11:00:00Z", "2025-11-25T09:00:00Z"]})
    assert asset.should_stop(old_df, forward_ctx) is True

    # Page with PRs updated after the watermark → should NOT stop
    new_df = pd.DataFrame({"updated_at": ["2025-12-05T14:00:00Z", "2025-12-01T09:00:00Z"]})
    assert asset.should_stop(new_df, forward_ctx) is False


def test_pull_requests_should_stop_noop_in_full_mode(monkeypatch):
    """In FULL mode, should_stop always returns False."""
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    from data_assets.assets.github.pull_requests import GitHubPullRequests

    asset = GitHubPullRequests()
    df = pd.DataFrame({"updated_at": ["2020-01-01T00:00:00Z"]})
    assert asset.should_stop(df, _ctx()) is False  # FULL mode, no stop
