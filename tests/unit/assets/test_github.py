"""Unit tests for all GitHub assets."""

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


class TestGitHubReposPrimaryKey:
    def test_pk_is_full_name(self, github_env):
        from data_assets.assets.github.repos import GitHubRepos

        assert GitHubRepos().primary_key == ["full_name"]


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


# ---------------------------------------------------------------------------
# GitHubMembers
# ---------------------------------------------------------------------------


class TestGitHubMembers:
    def test_build_request(self, github_env):
        from data_assets.assets.github.members import GitHubMembers

        spec = GitHubMembers().build_request(make_ctx())
        assert "/orgs/org-one/members" in spec.url
        assert spec.params["per_page"] == 100

    def test_parse_response(self, github_env):
        from data_assets.assets.github.members import GitHubMembers

        data = json.loads((FIXTURES / "members.json").read_text())
        df, state = GitHubMembers().parse_response(data)
        assert len(df) == 2
        assert list(df["login"]) == ["dev-alice", "dev-bob"]
        assert not state.has_more

    def test_primary_key(self, github_env):
        from data_assets.assets.github.members import GitHubMembers

        assert GitHubMembers().primary_key == ["login"]


# ---------------------------------------------------------------------------
# GitHubBranches
# ---------------------------------------------------------------------------


class TestGitHubBranches:
    def test_build_entity_request(self, github_env):
        from data_assets.assets.github.branches import GitHubBranches

        spec = GitHubBranches().build_entity_request("org-one/service-api", make_ctx())
        assert "/repos/org-one/service-api/branches" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.branches import GitHubBranches

        data = json.loads((FIXTURES / "branches.json").read_text())
        df, state = GitHubBranches().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["name"] == "main"
        assert df.iloc[0]["commit_sha"] == "abc123def456"

    def test_entity_key_column_set(self, github_env):
        from data_assets.assets.github.branches import GitHubBranches

        assert GitHubBranches().entity_key_column == "repo_full_name"

    def test_filter_entity_keys(self, github_env):
        from data_assets.assets.github.branches import GitHubBranches

        keys = ["org-one/repo", "org-two/repo"]
        assert GitHubBranches().filter_entity_keys(keys) == ["org-one/repo"]


# ---------------------------------------------------------------------------
# GitHubCommits
# ---------------------------------------------------------------------------


class TestGitHubCommits:
    def test_build_entity_request(self, github_env):
        from data_assets.assets.github.commits import GitHubCommits

        spec = GitHubCommits().build_entity_request("org-one/service-api", make_ctx())
        assert "/repos/org-one/service-api/commits" in spec.url

    def test_build_entity_request_with_since(self, github_env):
        from datetime import UTC, datetime

        from data_assets.assets.github.commits import GitHubCommits

        ctx = make_ctx(start_date=datetime(2025, 12, 1, tzinfo=UTC))
        spec = GitHubCommits().build_entity_request("org-one/service-api", ctx)
        assert "since" in spec.params

    def test_parse_response(self, github_env):
        from data_assets.assets.github.commits import GitHubCommits

        data = json.loads((FIXTURES / "commits.json").read_text())
        df, state = GitHubCommits().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["sha"] == "abc123def456789"
        assert df.iloc[0]["author_login"] == "dev-alice"

    def test_entity_key_column_set(self, github_env):
        from data_assets.assets.github.commits import GitHubCommits

        assert GitHubCommits().entity_key_column == "repo_full_name"
        assert GitHubCommits().date_column == "committer_date"


# ---------------------------------------------------------------------------
# GitHubWorkflows
# ---------------------------------------------------------------------------


class TestGitHubWorkflows:
    def test_build_entity_request(self, github_env):
        from data_assets.assets.github.workflows import GitHubWorkflows

        spec = GitHubWorkflows().build_entity_request("org-one/service-api", make_ctx())
        assert "/repos/org-one/service-api/actions/workflows" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.workflows import GitHubWorkflows

        data = json.loads((FIXTURES / "workflows.json").read_text())
        df, state = GitHubWorkflows().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["name"] == "CI"
        assert df.iloc[0]["path"] == ".github/workflows/ci.yml"
        assert state.total_records == 2

    def test_entity_key_column_set(self, github_env):
        from data_assets.assets.github.workflows import GitHubWorkflows

        assert GitHubWorkflows().entity_key_column == "repo_full_name"


# ---------------------------------------------------------------------------
# GitHubWorkflowRuns
# ---------------------------------------------------------------------------


class TestGitHubWorkflowRuns:
    def test_build_entity_request(self, github_env):
        from data_assets.assets.github.workflow_runs import GitHubWorkflowRuns

        spec = GitHubWorkflowRuns().build_entity_request("org-one/service-api", make_ctx())
        assert "/repos/org-one/service-api/actions/runs" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.workflow_runs import GitHubWorkflowRuns

        data = json.loads((FIXTURES / "workflow_runs.json").read_text())
        df, state = GitHubWorkflowRuns().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["conclusion"] == "success"
        assert df.iloc[1]["conclusion"] == "failure"
        assert state.total_records == 2

    def test_date_column(self, github_env):
        from data_assets.assets.github.workflow_runs import GitHubWorkflowRuns

        assert GitHubWorkflowRuns().date_column == "updated_at"


# ---------------------------------------------------------------------------
# GitHubWorkflowJobs
# ---------------------------------------------------------------------------


class TestGitHubWorkflowJobs:
    def test_build_entity_request_with_dict_key(self, github_env):
        from data_assets.assets.github.workflow_jobs import GitHubWorkflowJobs

        spec = GitHubWorkflowJobs().build_entity_request(
            {"id": 9000001, "repo_full_name": "org-one/service-api"}, make_ctx()
        )
        assert "/repos/org-one/service-api/actions/runs/9000001/jobs" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.workflow_jobs import GitHubWorkflowJobs

        data = json.loads((FIXTURES / "workflow_jobs.json").read_text())
        df, state = GitHubWorkflowJobs().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["name"] == "build"
        assert df.iloc[1]["name"] == "test"
        assert state.total_records == 2

    def test_parent_asset(self, github_env):
        from data_assets.assets.github.workflow_jobs import GitHubWorkflowJobs

        assert GitHubWorkflowJobs().parent_asset_name == "github_workflow_runs"

    def test_rejects_non_dict_entity_key(self, github_env):
        import pytest

        from data_assets.assets.github.workflow_jobs import GitHubWorkflowJobs

        with pytest.raises(TypeError, match="dict entity_key"):
            GitHubWorkflowJobs().build_entity_request(12345, make_ctx())


# ---------------------------------------------------------------------------
# GitHubUserDetails
# ---------------------------------------------------------------------------


class TestGitHubUserDetails:
    def test_build_entity_request(self, github_env):
        from data_assets.assets.github.user_details import GitHubUserDetails

        spec = GitHubUserDetails().build_entity_request("dev-alice", make_ctx())
        assert "/users/dev-alice" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.user_details import GitHubUserDetails

        data = json.loads((FIXTURES / "user_details.json").read_text())
        df, state = GitHubUserDetails().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["login"] == "dev-alice"
        assert df.iloc[0]["company"] == "Acme Corp"
        assert not state.has_more

    def test_parent_asset(self, github_env):
        from data_assets.assets.github.user_details import GitHubUserDetails

        assert GitHubUserDetails().parent_asset_name == "github_members"


# ---------------------------------------------------------------------------
# GitHubRunnerGroups
# ---------------------------------------------------------------------------


class TestGitHubRunnerGroups:
    def test_build_request(self, github_env):
        from data_assets.assets.github.runner_groups import GitHubRunnerGroups

        spec = GitHubRunnerGroups().build_request(make_ctx())
        assert "/orgs/org-one/actions/runner-groups" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.runner_groups import GitHubRunnerGroups

        data = json.loads((FIXTURES / "runner_groups.json").read_text())
        df, state = GitHubRunnerGroups().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["name"] == "Default"
        assert df.iloc[0]["default"] == "true"

    def test_primary_key(self, github_env):
        from data_assets.assets.github.runner_groups import GitHubRunnerGroups

        assert GitHubRunnerGroups().primary_key == ["id"]


# ---------------------------------------------------------------------------
# GitHubRunnerGroupRepos
# ---------------------------------------------------------------------------


class TestGitHubRunnerGroupRepos:
    def test_build_entity_request(self, github_env):
        from data_assets.assets.github.runner_group_repos import GitHubRunnerGroupRepos

        spec = GitHubRunnerGroupRepos().build_entity_request(2, make_ctx())
        assert "/orgs/org-one/actions/runner-groups/2/repositories" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.runner_group_repos import GitHubRunnerGroupRepos

        data = json.loads((FIXTURES / "runner_group_repos.json").read_text())
        df, state = GitHubRunnerGroupRepos().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["repo_full_name"] == "org-one/service-api"

    def test_entity_key_column(self, github_env):
        from data_assets.assets.github.runner_group_repos import GitHubRunnerGroupRepos

        assert GitHubRunnerGroupRepos().entity_key_column == "runner_group_id"


# ---------------------------------------------------------------------------
# GitHubRepoProperties
# ---------------------------------------------------------------------------


class TestGitHubRepoProperties:
    def test_build_entity_request(self, github_env):
        from data_assets.assets.github.repo_properties import GitHubRepoProperties

        spec = GitHubRepoProperties().build_entity_request("org-one/service-api", make_ctx())
        assert "/repos/org-one/service-api/properties/values" in spec.url

    def test_parse_response(self, github_env):
        from data_assets.assets.github.repo_properties import GitHubRepoProperties

        data = json.loads((FIXTURES / "repo_properties.json").read_text())
        df, state = GitHubRepoProperties().parse_response(data)
        assert len(df) == 3
        assert df.iloc[0]["property_name"] == "team"
        assert df.iloc[0]["value"] == "platform"
        assert not state.has_more

    def test_entity_key_column(self, github_env):
        from data_assets.assets.github.repo_properties import GitHubRepoProperties

        assert GitHubRepoProperties().entity_key_column == "repo_full_name"
