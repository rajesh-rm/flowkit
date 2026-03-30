"""Unit tests for GitHub asset build_request/parse_response."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

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
