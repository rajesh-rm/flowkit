"""Unit tests for GitHub assets: repos and pull requests."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from data_assets.core.enums import RunMode
from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "github"


# ---------------------------------------------------------------------------
# GitHubRepos
# ---------------------------------------------------------------------------


class TestGitHubReposBuildRequest:
    def test_first_org(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        spec = GitHubRepos().build_request(make_ctx())
        assert "/orgs/org-one/repos" in spec.url
        assert spec.params["page"] == 1

    def test_always_uses_first_org(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        spec = GitHubRepos().build_request(
            make_ctx(), checkpoint={"next_page": 1}
        )
        assert "/orgs/org-one/repos" in spec.url

    def test_next_page_from_checkpoint(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        spec = GitHubRepos().build_request(make_ctx(), checkpoint={"next_page": 3})
        assert spec.params["page"] == 3

    def test_next_page_none_defaults_to_1(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        spec = GitHubRepos().build_request(
            make_ctx(), checkpoint={"next_page": None}
        )
        assert spec.params["page"] == 1


class TestGitHubReposParseResponse:
    def test_happy_path(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        data = json.loads((FIXTURES / "repos_org1.json").read_text())
        df, state = GitHubRepos().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["owner_login"] == "org-one"
        assert not state.has_more

    def test_empty_response(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        df, state = GitHubRepos().parse_response([])
        assert len(df) == 0
        assert not state.has_more

    def test_full_page_signals_has_more(self, github_env):
        """100 results (= page_size) should signal has_more=True."""
        from data_assets.assets.github.repos import GitHubRepos

        fake_repos = [{"id": i, "full_name": f"org/repo-{i}", "owner": {"login": "org"}}
                      for i in range(100)]
        _, state = GitHubRepos().parse_response(fake_repos)
        assert state.has_more is True


# ---------------------------------------------------------------------------
# GitHubPullRequests
# ---------------------------------------------------------------------------


class TestGitHubPullRequestsBuildRequest:
    def test_entity_request(self, github_env):
        from data_assets.assets.github.pull_requests import GitHubPullRequests

        spec = GitHubPullRequests().build_entity_request(
            "org-one/service-api", make_ctx()
        )
        assert "/repos/org-one/service-api/pulls" in spec.url
        assert spec.params["state"] == "all"
        assert spec.params["sort"] == "updated"


class TestGitHubPullRequestsParseResponse:
    def test_happy_path(self, github_env):
        from data_assets.assets.github.pull_requests import GitHubPullRequests

        data = json.loads((FIXTURES / "pull_requests.json").read_text())
        df, state = GitHubPullRequests().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["user_login"] == "dev-alice"


class TestGitHubReposPrimaryKey:
    def test_pk_is_full_name(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        assert GitHubRepos().primary_key == ["full_name"]


class TestGitHubPullRequestsFilterEntityKeys:
    def test_filters_to_current_org(self, github_env):
        from data_assets.assets.github.pull_requests import GitHubPullRequests

        keys = ["org-one/service-api", "org-one/web-ui", "org-two/data-pipeline"]
        filtered = GitHubPullRequests().filter_entity_keys(keys)
        assert filtered == ["org-one/service-api", "org-one/web-ui"]

    def test_no_org_returns_all(self, github_env, monkeypatch):
        from data_assets.assets.github.pull_requests import GitHubPullRequests

        monkeypatch.setenv("GITHUB_ORGS", "")
        keys = ["org-one/repo", "org-two/repo"]
        assert GitHubPullRequests().filter_entity_keys(keys) == keys

    def test_single_org(self, github_env, monkeypatch):
        from data_assets.assets.github.pull_requests import GitHubPullRequests

        monkeypatch.setenv("GITHUB_ORGS", "org-two")
        keys = ["org-one/repo-a", "org-two/repo-b", "org-two/repo-c"]
        filtered = GitHubPullRequests().filter_entity_keys(keys)
        assert filtered == ["org-two/repo-b", "org-two/repo-c"]


class TestGitHubPullRequestsShouldStop:
    def test_stops_in_forward_mode_past_watermark(self, github_env):
        from dataclasses import replace
        from datetime import UTC, datetime

        from data_assets.assets.github.pull_requests import GitHubPullRequests

        ctx = replace(
            make_ctx(start_date=datetime(2025, 12, 1, tzinfo=UTC)),
            mode=RunMode.FORWARD,
        )
        old_df = pd.DataFrame(
            {"updated_at": ["2025-11-28T11:00:00Z", "2025-11-25T09:00:00Z"]}
        )
        assert GitHubPullRequests().should_stop(old_df, ctx) is True

    def test_does_not_stop_before_watermark(self, github_env):
        from dataclasses import replace
        from datetime import UTC, datetime

        from data_assets.assets.github.pull_requests import GitHubPullRequests

        ctx = replace(
            make_ctx(start_date=datetime(2025, 12, 1, tzinfo=UTC)),
            mode=RunMode.FORWARD,
        )
        new_df = pd.DataFrame(
            {"updated_at": ["2025-12-05T14:00:00Z", "2025-12-01T09:00:00Z"]}
        )
        assert GitHubPullRequests().should_stop(new_df, ctx) is False

    def test_noop_in_full_mode(self, github_env):
        from data_assets.assets.github.pull_requests import GitHubPullRequests

        df = pd.DataFrame({"updated_at": ["2020-01-01T00:00:00Z"]})
        assert GitHubPullRequests().should_stop(df, make_ctx()) is False

    def test_noop_on_empty_dataframe(self, github_env):
        from data_assets.assets.github.pull_requests import GitHubPullRequests

        empty = pd.DataFrame(columns=["updated_at"])
        assert GitHubPullRequests().should_stop(empty, make_ctx()) is False
