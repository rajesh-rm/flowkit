"""Unit tests for the GitHub deployments asset (GraphQL transport)."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from data_assets.core.enums import RunMode
from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "github"


# ---------------------------------------------------------------------------
# Class wiring — PK, indexes, entity_key_map, columns
# ---------------------------------------------------------------------------


class TestGitHubDeploymentsWiring:
    def test_registered_with_composite_pk(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        asset = GitHubDeployments()
        assert asset.name == "github_deployments"
        assert asset.target_table == "github_deployments"
        assert asset.primary_key == ["deployment_id", "organization"]

    def test_entity_key_map_injects_three_columns(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        assert GitHubDeployments().entity_key_column is None
        assert GitHubDeployments().entity_key_map == {
            "owner": "organization",
            "name": "repo_name",
            "full_name": "org_repo_key",
        }

    def test_declared_indexes_include_deployment_id_and_created_at(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        idx_cols = {idx.columns for idx in GitHubDeployments().indexes}
        assert ("deployment_id",) in idx_cols
        assert ("created_at",) in idx_cols

    def test_description_max_length_accommodates_truncation_marker(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        # 2000 head + "[truncated]" (11) + 2000 tail = 4011 worst case. 4100 gives buffer.
        assert GitHubDeployments().column_max_lengths["description"] >= 4011


# ---------------------------------------------------------------------------
# filter_entity_keys — GITHUB_ORGS scoping + string→dict reshape
# ---------------------------------------------------------------------------


class TestFilterEntityKeys:
    def test_scopes_to_current_org_and_builds_dicts(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        keys = ["org-one/svc-a", "org-one/svc-b", "org-two/svc-c"]
        filtered = GitHubDeployments().filter_entity_keys(keys)
        assert filtered == [
            {"owner": "org-one", "name": "svc-a", "full_name": "org-one/svc-a"},
            {"owner": "org-one", "name": "svc-b", "full_name": "org-one/svc-b"},
        ]

    def test_respects_org_switch(self, github_env, monkeypatch):
        from data_assets.assets.github.deployments import GitHubDeployments

        monkeypatch.setenv("GITHUB_ORGS", "org-two")
        keys = ["org-one/a", "org-two/b", "org-two/c"]
        filtered = GitHubDeployments().filter_entity_keys(keys)
        assert [r["full_name"] for r in filtered] == ["org-two/b", "org-two/c"]

    def test_drops_malformed_keys(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        filtered = GitHubDeployments().filter_entity_keys(
            ["org-one/valid", "no-slash", ""]
        )
        assert [r["full_name"] for r in filtered] == ["org-one/valid"]

    def test_malformed_keys_emit_warning(self, github_env, caplog, monkeypatch):
        """Dropped parent entries must be observable in logs, not silent.

        Sets GITHUB_ORGS to empty so `filter_to_current_org` is a passthrough —
        malformed entries (non-strings, strings without '/') then reach our own
        filter_entity_keys skip branch where the WARNING is emitted. This is
        exactly the forensic path operators need if parent data ever drifts.
        """
        import logging

        monkeypatch.setenv("GITHUB_ORGS", "")
        from data_assets.assets.github.deployments import GitHubDeployments

        with caplog.at_level(
            logging.WARNING,
            logger="data_assets.assets.github.deployments",
        ):
            GitHubDeployments().filter_entity_keys(
                ["org-one/valid", "no-slash", 42]
            )
        messages = [
            rec.getMessage()
            for rec in caplog.records
            if rec.levelno == logging.WARNING
            and rec.name == "data_assets.assets.github.deployments"
        ]
        assert any("no-slash" in m for m in messages), messages
        assert any("42" in m for m in messages), messages


# ---------------------------------------------------------------------------
# build_entity_request — POST shape, cursor threading, DESC order
# ---------------------------------------------------------------------------


class TestBuildEntityRequest:
    def _entity_key(self):
        return {
            "owner": "org-one",
            "name": "devops-tooling",
            "full_name": "org-one/devops-tooling",
        }

    def test_method_and_url_are_graphql_post(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        spec = GitHubDeployments().build_entity_request(
            self._entity_key(), make_ctx(),
        )
        assert spec.method == "POST"
        assert spec.url.endswith("/graphql")

    def test_body_contains_query_and_variables_first_call(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        spec = GitHubDeployments().build_entity_request(
            self._entity_key(), make_ctx(), checkpoint=None,
        )
        body = spec.body
        assert body is not None
        assert "query" in body and "deployments" in body["query"]
        assert body["variables"]["owner"] == "org-one"
        assert body["variables"]["repo"] == "devops-tooling"
        assert body["variables"]["pageSize"] == 50
        assert body["variables"]["cursor"] is None
        assert body["variables"]["orderDirection"] == "DESC"

    def test_body_threads_cursor_from_checkpoint(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        spec = GitHubDeployments().build_entity_request(
            self._entity_key(), make_ctx(), checkpoint={"cursor": "OPAQUE=="},
        )
        assert spec.body["variables"]["cursor"] == "OPAQUE=="

    def test_no_query_params_and_sends_auth_compatible_headers(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        spec = GitHubDeployments().build_entity_request(
            self._entity_key(), make_ctx(),
        )
        assert spec.params is None
        # Auth is merged by APIClient; asset should only set the accept header.
        assert spec.headers == {"Accept": "application/vnd.github+json"}


# ---------------------------------------------------------------------------
# parse_response — happy path, empty, errors, missing keys, null parents
# ---------------------------------------------------------------------------


class TestParseResponseHappyPath:
    def test_page1_extracts_ten_rows(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        data = json.loads((FIXTURES / "deployments_graphql_page1.json").read_text())
        df, state = GitHubDeployments().parse_response(data)

        assert len(df) == 10
        assert state.has_more is True
        assert state.cursor == (
            "Y3Vyc29yOnYyOpK0MjAyNS0wNC0wOVQxNzo1NTowNFrOjlNLZw=="
        )

    def test_column_mapping(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        data = json.loads((FIXTURES / "deployments_graphql_page1.json").read_text())
        df, _ = GitHubDeployments().parse_response(data)

        first = df.iloc[0]
        assert first["deployment_id"] == 2408327264
        assert first["environment"] == "nginx-update"
        assert first["state"] == "INACTIVE"
        assert first["latest_status"] == "INACTIVE"
        assert first["creator_login"] == "user111"
        assert first["sha"] == "be6d78d6729938f67adfdd29b8fb729e32346937"
        assert first["created_at"] == "2025-04-15T18:16:10Z"

    def test_injection_columns_not_populated_by_parse(self, github_env):
        """organization / repo_name / org_repo_key are injected later, not here."""
        from data_assets.assets.github.deployments import GitHubDeployments

        data = json.loads((FIXTURES / "deployments_graphql_page1.json").read_text())
        df, _ = GitHubDeployments().parse_response(data)
        assert df["organization"].isnull().all()
        assert df["repo_name"].isnull().all()
        assert df["org_repo_key"].isnull().all()
        assert df["source_url"].isnull().all()  # computed by transform()


class TestParseResponseNullParents:
    def test_null_creator_yields_none_creator_login(self, github_env):
        """Row with creator: null should not raise; creator_login becomes None."""
        from data_assets.assets.github.deployments import GitHubDeployments

        data = json.loads((FIXTURES / "deployments_graphql_page1.json").read_text())
        df, _ = GitHubDeployments().parse_response(data)
        # Fixture row 4 (databaseId=2404802687) has creator: null, latestStatus: null.
        # Pandas coerces None in object columns to NaN — use pd.isna() to assert.
        row = df[df["deployment_id"] == 2404802687].iloc[0]
        assert pd.isna(row["creator_login"])
        assert pd.isna(row["latest_status"])


class TestParseResponseEmpty:
    def test_empty_nodes_returns_empty_frame_with_all_columns(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        data = json.loads((FIXTURES / "deployments_graphql_empty.json").read_text())
        asset = GitHubDeployments()
        df, state = asset.parse_response(data)

        assert len(df) == 0
        assert state.has_more is False
        assert list(df.columns) == [c.name for c in asset.columns]


class TestParseResponseErrors:
    def test_top_level_errors_raises_with_upstream_message(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        data = json.loads((FIXTURES / "deployments_graphql_errors.json").read_text())
        with pytest.raises(ValueError, match="INSUFFICIENT_SCOPES|required scopes"):
            GitHubDeployments().parse_response(data)


class TestParseResponseMissingKey:
    def test_missing_database_id_raises_missing_key_error(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments
        from data_assets.validation.missing_keys import MissingKeyError

        data = json.loads((FIXTURES / "deployments_graphql_page1.json").read_text())
        # Strip databaseId from the first node
        del data["data"]["repository"]["deployments"]["nodes"][0]["databaseId"]

        with pytest.raises(MissingKeyError) as exc:
            GitHubDeployments().parse_response(data)
        assert exc.value.column == "deployment_id"
        assert exc.value.field_path == "databaseId"

    def test_missing_connection_wrapper_yields_zero_rows(self, github_env):
        """A broken repository/deployments wrapper is tolerated, not failed."""
        from data_assets.assets.github.deployments import GitHubDeployments

        df, state = GitHubDeployments().parse_response({"data": {"repository": None}})
        assert len(df) == 0
        assert state.has_more is False


class TestParseResponseNonDictGuard:
    """A non-dict top-level body must raise a typed, asset-named ValueError."""

    def test_list_body_raises_with_asset_name(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        with pytest.raises(ValueError, match="github_deployments.*not a JSON object"):
            GitHubDeployments().parse_response([{"some": "list"}])

    def test_string_body_raises(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        with pytest.raises(ValueError, match="not a JSON object.*str"):
            GitHubDeployments().parse_response("Service Unavailable")

    def test_none_body_raises(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        with pytest.raises(ValueError, match="not a JSON object.*NoneType"):
            GitHubDeployments().parse_response(None)


# ---------------------------------------------------------------------------
# transform — source_url + description truncation
# ---------------------------------------------------------------------------


class TestTransform:
    def _df(self, desc: str | None, deployment_id: int = 1):
        return pd.DataFrame([{
            "deployment_id": deployment_id,
            "organization": "org-one",
            "repo_name": "svc",
            "org_repo_key": "org-one/svc",
            "environment": "prod",
            "description": desc,
            "state": "ACTIVE",
            "latest_status": "SUCCESS",
            "creator_login": "user111",
            "sha": "abc",
            "created_at": "2025-04-15T18:16:10Z",
            "updated_at": "2025-04-15T19:53:30Z",
            "source_url": None,
        }])

    def test_source_url_computed_from_key_and_id(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        out = GitHubDeployments().transform(self._df(desc="short", deployment_id=42))
        assert out.iloc[0]["source_url"] == (
            "https://github.com/org-one/svc/deployments/42"
        )

    def test_description_short_unchanged(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        short = "a" * 3999
        out = GitHubDeployments().transform(self._df(desc=short))
        assert out.iloc[0]["description"] == short

    def test_description_at_exact_limit_unchanged(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        at_limit = "b" * 4000
        out = GitHubDeployments().transform(self._df(desc=at_limit))
        assert out.iloc[0]["description"] == at_limit

    def test_description_long_truncated_with_middle_marker(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        head = "H" * 3000
        tail = "T" * 3000
        long_desc = head + tail  # 6000 chars — well over the 4000 limit
        out = GitHubDeployments().transform(self._df(desc=long_desc))
        result = out.iloc[0]["description"]

        assert len(result) == 2000 + len("[truncated]") + 2000
        assert result.startswith("H" * 2000)
        assert result.endswith("T" * 2000)
        assert "[truncated]" in result

    def test_description_null_stays_null(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        out = GitHubDeployments().transform(self._df(desc=None))
        assert out.iloc[0]["description"] is None

    def test_empty_dataframe_noop(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        empty = pd.DataFrame(columns=[c.name for c in GitHubDeployments().columns])
        out = GitHubDeployments().transform(empty)
        assert len(out) == 0

    def test_truncation_count_logged_when_fires(self, github_env, caplog):
        """Truncation must emit a single INFO log per run with the exact count."""
        import logging

        from data_assets.assets.github.deployments import GitHubDeployments

        # Three rows: two oversized (should truncate+count), one short (unchanged).
        def _row(desc, dep_id):
            return {
                "deployment_id": dep_id,
                "organization": "org-one",
                "repo_name": "svc",
                "org_repo_key": "org-one/svc",
                "environment": "prod",
                "description": desc,
                "state": "ACTIVE",
                "latest_status": "SUCCESS",
                "creator_login": "u",
                "sha": "abc",
                "created_at": "2025-04-15T18:16:10Z",
                "updated_at": "2025-04-15T19:53:30Z",
                "source_url": None,
            }
        df = pd.DataFrame([
            _row("x" * 5000, 1),   # truncated
            _row("short", 2),       # untouched
            _row("y" * 4500, 3),   # truncated
        ])

        with caplog.at_level(
            logging.INFO, logger="data_assets.assets.github.deployments",
        ):
            GitHubDeployments().transform(df)

        info_msgs = [
            rec.getMessage()
            for rec in caplog.records
            if rec.levelno == logging.INFO
            and rec.name == "data_assets.assets.github.deployments"
        ]
        trunc_msgs = [m for m in info_msgs if "truncated" in m]
        assert len(trunc_msgs) == 1, trunc_msgs
        assert "truncated 2 description" in trunc_msgs[0]
        assert "4000 chars" in trunc_msgs[0]

    def test_no_truncation_log_when_none_over_limit(self, github_env, caplog):
        """Runs with no oversized descriptions emit no truncation log (no noise)."""
        import logging

        from data_assets.assets.github.deployments import GitHubDeployments

        with caplog.at_level(
            logging.INFO, logger="data_assets.assets.github.deployments",
        ):
            GitHubDeployments().transform(self._df(desc="short"))

        info_msgs = [
            rec.getMessage()
            for rec in caplog.records
            if rec.levelno == logging.INFO
            and rec.name == "data_assets.assets.github.deployments"
        ]
        assert not any("truncated" in m for m in info_msgs), info_msgs


# ---------------------------------------------------------------------------
# should_stop — pull_upto_days cap + watermark
# ---------------------------------------------------------------------------


class TestShouldStop:
    def test_returns_false_on_empty_frame(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        assert GitHubDeployments().should_stop(pd.DataFrame(), make_ctx()) is False

    def test_stops_when_oldest_precedes_pull_upto_cap(self, github_env):
        """FULL mode (no watermark): threshold is today - pull_upto_days."""
        from data_assets.assets.github.deployments import GitHubDeployments

        asset = GitHubDeployments()
        very_old = datetime.now(UTC) - timedelta(days=asset.pull_upto_days + 10)
        df = pd.DataFrame({
            "created_at": [very_old.isoformat(), (very_old + timedelta(days=1)).isoformat()],
        })
        assert asset.should_stop(df, make_ctx()) is True

    def test_does_not_stop_when_page_within_cap(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        recent = datetime.now(UTC) - timedelta(days=1)
        df = pd.DataFrame({"created_at": [recent.isoformat()]})
        assert GitHubDeployments().should_stop(df, make_ctx()) is False

    def test_stops_on_watermark_in_forward_mode(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        watermark = datetime.now(UTC) - timedelta(days=7)
        older = watermark - timedelta(days=1)
        ctx = replace(
            make_ctx(start_date=watermark),
            mode=RunMode.FORWARD,
        )
        df = pd.DataFrame({"created_at": [older.isoformat()]})
        assert GitHubDeployments().should_stop(df, ctx) is True

    def test_does_not_stop_when_all_newer_than_watermark(self, github_env):
        from data_assets.assets.github.deployments import GitHubDeployments

        watermark = datetime.now(UTC) - timedelta(days=7)
        newer = datetime.now(UTC) - timedelta(days=1)
        ctx = replace(
            make_ctx(start_date=watermark),
            mode=RunMode.FORWARD,
        )
        df = pd.DataFrame({"created_at": [newer.isoformat()]})
        assert GitHubDeployments().should_stop(df, ctx) is False
