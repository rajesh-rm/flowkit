"""Unit tests for SonarQube assets: projects (RestAsset), issues (APIAsset), measures."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from tests.unit.conftest import make_ctx

FIXTURES = Path(__file__).parent.parent.parent / "fixtures" / "sonarqube"


# ---------------------------------------------------------------------------
# SonarQubeProjects (RestAsset — declarative, /api/components/search)
# ---------------------------------------------------------------------------


class TestSonarQubeProjects:
    def test_is_rest_asset(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects
        from data_assets.core.rest_asset import RestAsset

        assert issubclass(SonarQubeProjects, RestAsset)
        asset = SonarQubeProjects()
        assert asset.endpoint == "/api/components/search"
        assert asset.response_path == "components"

    def test_build_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        spec = SonarQubeProjects().build_request(make_ctx())
        assert spec.url == "https://sonar.test/api/components/search"
        assert spec.params["ps"] == 100
        assert spec.params["p"] == 1
        assert spec.params["qualifiers"] == "TRK"

    def test_build_request_with_checkpoint(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        spec = SonarQubeProjects().build_request(
            make_ctx(), checkpoint={"next_page": 3}
        )
        assert spec.params["p"] == 3
        assert spec.params["qualifiers"] == "TRK"

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        data = json.loads((FIXTURES / "projects_page1.json").read_text())
        df, state = SonarQubeProjects().parse_response(data)
        assert len(df) == 3
        assert "key" in df.columns
        assert "name" in df.columns
        assert "qualifier" in df.columns
        assert not state.has_more
        assert state.total_records == 3

    def test_primary_key_is_key(self, sonarqube_env):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects

        assert SonarQubeProjects().primary_key == ["key"]


# ---------------------------------------------------------------------------
# SonarQubeIssues (APIAsset — custom, entity-parallel, /api/issues/search)
# ---------------------------------------------------------------------------


class TestSonarQubeIssues:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        spec = SonarQubeIssues().build_entity_request("proj-alpha", make_ctx())
        assert spec.url == "https://sonar.test/api/issues/search"
        assert spec.params["componentKeys"] == "proj-alpha"
        assert spec.params["s"] == "UPDATE_DATE"
        assert spec.params["asc"] == "true"

    def test_tracks_watermark_on_update_date(self, sonarqube_env):
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        assert SonarQubeIssues().date_column == "update_date"

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        data = json.loads((FIXTURES / "issues_proj_alpha.json").read_text())
        df, state = SonarQubeIssues().parse_response(data)
        assert len(df) == 5
        assert "creation_date" in df.columns
        assert "update_date" in df.columns
        assert not state.has_more

    def test_parse_response_renames_date_fields(self, sonarqube_env):
        """API returns creationDate/updateDate, asset renames to snake_case."""
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        data = json.loads((FIXTURES / "issues_proj_alpha.json").read_text())
        df, _ = SonarQubeIssues().parse_response(data)
        assert "creationDate" not in df.columns
        assert "updateDate" not in df.columns
        assert "creation_date" in df.columns
        assert "update_date" in df.columns

    def test_build_request_raises_for_entity_parallel(self, sonarqube_env):
        """Entity-parallel assets should use build_entity_request, not build_request."""
        import pytest

        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        with pytest.raises(NotImplementedError):
            SonarQubeIssues().build_request(make_ctx())

    def test_pagination_state(self, sonarqube_env):
        """Verify pagination math from real-style response."""
        from data_assets.assets.sonarqube.issues import SonarQubeIssues

        data = {
            "paging": {"pageIndex": 1, "pageSize": 100, "total": 250},
            "issues": [{"key": f"issue-{i}", "rule": "r", "severity": "MAJOR",
                        "component": "c", "project": "p", "status": "OPEN",
                        "type": "BUG", "creationDate": "2025-01-01T00:00:00+0000",
                        "updateDate": "2025-01-01T00:00:00+0000"} for i in range(100)],
        }
        _, state = SonarQubeIssues().parse_response(data)
        assert state.has_more is True
        assert state.total_pages == 3
        assert state.next_page == 2
        assert state.total_records == 250


# ---------------------------------------------------------------------------
# SonarQubeMeasures (APIAsset — entity-parallel, /api/measures/component)
# ---------------------------------------------------------------------------


class TestSonarQubeMeasures:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        entity_key = {"project_key": "proj-alpha", "name": "main"}
        spec = SonarQubeMeasures().build_entity_request(entity_key, make_ctx())
        assert spec.url == "https://sonar.test/api/measures/component"
        assert spec.params["component"] == "proj-alpha"
        assert spec.params["branch"] == "main"
        assert "ncloc" in spec.params["metricKeys"]
        assert "bugs" in spec.params["metricKeys"]
        assert "new_coverage" in spec.params["metricKeys"]
        assert "new_line_coverage" in spec.params["metricKeys"]

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        data = json.loads((FIXTURES / "measures_proj_alpha.json").read_text())
        df, state = SonarQubeMeasures().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["project_key"] == "proj-alpha"
        assert df.iloc[0]["ncloc"] == "12500"
        assert df.iloc[0]["bugs"] == "3"
        assert df.iloc[0]["coverage"] == "87.5"
        assert df.iloc[0]["new_coverage"] == "92.0"
        assert df.iloc[0]["new_lines_to_cover"] == "150"
        assert df.iloc[0]["new_line_coverage"] == "88.5"
        assert state.has_more is False

    def test_parse_response_has_collected_at(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        data = json.loads((FIXTURES / "measures_proj_alpha.json").read_text())
        df, _ = SonarQubeMeasures().parse_response(data)
        assert "collected_at" in df.columns
        assert pd.notna(df.iloc[0]["collected_at"])

    def test_parse_empty_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        df, state = SonarQubeMeasures().parse_response({"component": {}})
        assert len(df) == 0
        assert state.has_more is False

    def test_parent_asset(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        assert SonarQubeMeasures().parent_asset_name == "sonarqube_branches"

    def test_primary_key(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        assert SonarQubeMeasures().primary_key == ["project_key", "branch"]

    def test_entity_key_map(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures import SonarQubeMeasures

        asset = SonarQubeMeasures()
        assert asset.entity_key_column is None
        assert asset.entity_key_map == {"name": "branch"}


# ---------------------------------------------------------------------------
# SonarQubeProjects — Sharded extraction for the 10k ES limit
# ---------------------------------------------------------------------------


def _make_probe_response(total: int) -> dict:
    """Build a minimal probe response (ps=1) with the given total."""
    components = []
    if total > 0:
        components.append({"key": "probe-key", "name": "Probe", "qualifier": "TRK"})
    return {"paging": {"pageIndex": 1, "pageSize": 1, "total": total}, "components": components}


def _make_page_response(
    projects: list[tuple[str, str]],
    page_index: int = 1,
    page_size: int = 100,
    total: int | None = None,
) -> dict:
    """Build a paginated response from a list of (key, name) tuples."""
    if total is None:
        total = len(projects)
    return {
        "paging": {"pageIndex": page_index, "pageSize": page_size, "total": total},
        "components": [
            {"key": k, "name": n, "qualifier": "TRK"} for k, n in projects
        ],
    }


class TestSonarQubeProjectsSharding:
    """Tests for the sharded extraction logic in SonarQubeProjects.extract()."""

    def _make_asset(self):
        from data_assets.assets.sonarqube.projects import SonarQubeProjects
        return SonarQubeProjects()

    # -- attribute / config tests --------------------------------------

    def test_parallel_mode_is_none(self, sonarqube_env):
        from data_assets.core.enums import ParallelMode
        assert self._make_asset().parallel_mode == ParallelMode.NONE

    # -- normal path (below threshold) ---------------------------------

    def test_extract_below_threshold_paginates_normally(self, sonarqube_env):
        """When total <= 9900, extract paginates without the q param."""
        asset = self._make_asset()
        projects = [("k1", "Name One"), ("k2", "Name Two"), ("k3", "Name Three")]
        page_resp = _make_page_response(projects, total=3)

        mock_client = MagicMock()
        # First call: probe (ps=1), second: paginate page 1
        mock_client.request.side_effect = [
            _make_probe_response(total=3),
            page_resp,
        ]

        engine = MagicMock()
        ctx = make_ctx()

        with patch.object(asset, "_create_client", return_value=mock_client), \
             patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=3):
            rows = asset.extract(engine, "tmp_tbl", ctx)

        assert rows == 3
        # Paginate call should NOT have a 'q' param
        paginate_spec = mock_client.request.call_args_list[1][0][0]
        assert "q" not in paginate_spec.params

    # -- sharded path (above threshold) --------------------------------

    def test_extract_above_threshold_triggers_sharding(self, sonarqube_env):
        """When total > 9900, extract uses q-param shard probes."""
        import pytest

        asset = self._make_asset()

        # Probe returns large total; all shard probes return 0 except 'aa'
        def mock_request(spec):
            params = spec.params
            if params.get("ps") == 1 and "q" not in params:
                return _make_probe_response(total=10_500)
            if params.get("ps") == 1 and "q" in params:
                if params["q"] == "aa":
                    return _make_probe_response(total=50)
                return _make_probe_response(total=0)
            return _make_page_response(
                [("aa-1", "Project AA One"), ("aa-2", "Project AA Two")],
                total=2,
            )

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        # Only 2 of 10,500 found → shortfall guard raises; we verify shard probes happened
        with patch.object(asset, "_create_client", return_value=mock_client), \
             patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=2), \
             pytest.raises(ValueError, match="shortfall"):
            asset.extract(engine, "tmp_tbl", make_ctx())

        shard_calls = [
            c[0][0] for c in mock_client.request.call_args_list
            if c[0][0].params.get("ps") == 1 and "q" in c[0][0].params
        ]
        assert len(shard_calls) > 0
        assert all("q" in c.params for c in shard_calls)

    # -- deduplication -------------------------------------------------

    def test_dedup_across_shards(self, sonarqube_env):
        """Same project key from multiple shards is written only once."""
        asset = self._make_asset()
        seen_keys: set[str] = set()
        engine = MagicMock()

        resp1 = _make_page_response([("proj-dup", "Dup Proj"), ("proj-a", "A Proj")])
        resp2 = _make_page_response([("proj-dup", "Dup Proj"), ("proj-b", "B Proj")])

        mock_client = MagicMock()
        written_frames: list[pd.DataFrame] = []

        def capture_write(_engine, _table, df):
            written_frames.append(df.copy())
            return len(df)

        with patch("data_assets.assets.sonarqube.projects.write_to_temp", side_effect=capture_write):
            mock_client.request.return_value = resp1
            asset._paginate_shard(mock_client, engine, "tmp", make_ctx(), "ab", seen_keys)

            mock_client.request.return_value = resp2
            asset._paginate_shard(mock_client, engine, "tmp", make_ctx(), "du", seen_keys)

        all_keys = pd.concat(written_frames)["key"].tolist()
        assert sorted(all_keys) == ["proj-a", "proj-b", "proj-dup"]
        assert len(all_keys) == 3  # no duplicates

    # -- multi-page pagination -----------------------------------------

    def test_paginate_shard_handles_multiple_pages(self, sonarqube_env):
        """Pagination loop fetches all pages and deduplicates across them."""
        asset = self._make_asset()
        seen_keys: set[str] = set()
        written_frames: list[pd.DataFrame] = []

        call_count = [0]

        def mock_request(spec):
            call_count[0] += 1
            page = spec.params.get("p", 1)
            if page == 1:
                # total=200 so total_pages=ceil(200/100)=2 → has_more=True
                return _make_page_response(
                    [("k1", "Name 1"), ("k2", "Name 2")],
                    page_index=1, total=200,
                )
            # Page 2: k2 appears again (cross-page dupe) + k3 is new
            return _make_page_response(
                [("k2", "Name 2"), ("k3", "Name 3")],
                page_index=2, total=200,
            )

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request

        def capture_write(_engine, _table, df):
            written_frames.append(df.copy())
            return len(df)

        with patch("data_assets.assets.sonarqube.projects.write_to_temp", side_effect=capture_write):
            names = asset._paginate_shard(
                mock_client, MagicMock(), "tmp", make_ctx(), "ab", seen_keys,
            )

        # Should have fetched 2 pages
        assert call_count[0] == 2
        # k2 appeared on both pages but should be written only once
        all_keys = pd.concat(written_frames)["key"].tolist()
        assert sorted(all_keys) == ["k1", "k2", "k3"]
        assert len(all_keys) == 3
        # Names returned for early-termination tracking
        assert names == {"Name 1", "Name 2", "Name 3"}

    # -- zero result shard skipped -------------------------------------

    def test_zero_result_shard_skipped(self, sonarqube_env):
        """Shard probe returning 0 triggers no pagination calls."""
        asset = self._make_asset()
        mock_client = MagicMock()
        mock_client.request.return_value = _make_probe_response(total=0)

        result = asset._probe(mock_client, make_ctx(), q="zz")
        assert result == 0

    # -- deep shard (extends to 3-char) --------------------------------

    def test_deep_shard_extends_to_three_chars(self, sonarqube_env):
        """2-char prefix with total > 9900 extends to 3-char sub-prefixes."""
        asset = self._make_asset()
        seen_keys: set[str] = set()

        call_log: list[str] = []

        def mock_request(spec):
            q = spec.params.get("q", "")
            ps = spec.params.get("ps")
            if ps == 1:
                call_log.append(f"probe:{q}")
                if len(q) == 3:
                    if q == "aba":
                        return _make_probe_response(total=50)
                    return _make_probe_response(total=0)
                return _make_probe_response(total=0)
            call_log.append(f"paginate:{q}")
            return _make_page_response(
                [("aba-1", "ABA One"), ("aba-2", "ABA Two")], total=2,
            )

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        with patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=2):
            names = asset._extend_shard(
                mock_client, engine, "tmp", make_ctx(),
                parent_prefix="ab", parent_total=50, seen_keys=seen_keys,
            )

        three_char_probes = [c for c in call_log if c.startswith("probe:ab") and len(c.split(":")[1]) == 3]
        assert len(three_char_probes) > 0
        assert "paginate:aba" in call_log
        assert "ABA One" in names

    # -- early termination ---------------------------------------------

    def test_early_termination_stops_when_n_names_collected(self, sonarqube_env):
        """Stops iterating sub-prefixes once parent_total unique names reached."""
        asset = self._make_asset()
        seen_keys: set[str] = set()
        probed_prefixes: list[str] = []

        def mock_request(spec):
            q = spec.params.get("q", "")
            ps = spec.params.get("ps")
            if ps == 1:
                probed_prefixes.append(q)
                if len(q) == 3:
                    return _make_probe_response(total=3)
                return _make_probe_response(total=0)
            return _make_page_response([
                (f"{q}-1", f"Name {q} 1"),
                (f"{q}-2", f"Name {q} 2"),
                (f"{q}-3", f"Name {q} 3"),
            ], total=3)

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        with patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=3):
            names = asset._extend_shard(
                mock_client, engine, "tmp", make_ctx(),
                parent_prefix="ab", parent_total=6, seen_keys=seen_keys,
            )

        assert len(names) >= 6
        three_char_probes = [p for p in probed_prefixes if len(p) == 3]
        assert len(three_char_probes) < 36

    # -- reconciliation ------------------------------------------------
    # Patch _SAFE_LIMIT to 2 so sharding triggers with small totals,
    # while shard results (≤ 2) still get paginated instead of extended.

    def test_reconciliation_warns_on_small_shortfall(self, sonarqube_env, caplog):
        """Logs a warning (but succeeds) when shortfall is within 5% tolerance."""
        asset = self._make_asset()

        def mock_request(spec):
            params = spec.params
            if params.get("ps") == 1 and "q" not in params:
                # Expect 4 projects but only 'aa' shard returns data (2 of 4 = 50%).
                # With _SAFE_LIMIT patched to 50, total=52 triggers sharding
                # and the 2-project shortfall is ~3.8%, within 5% tolerance.
                return _make_probe_response(total=52)
            if params.get("ps") == 1:
                if params.get("q") == "aa":
                    return _make_probe_response(total=50)
                return _make_probe_response(total=0)
            # Paginate 'aa' — return 50 unique projects
            page = params.get("p", 1)
            start = (page - 1) * 100
            projects = [(f"p-{i}", f"Proj {i}") for i in range(start, min(start + 100, 50))]
            return _make_page_response(projects, page_index=page, total=50)

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        with patch.object(asset, "_create_client", return_value=mock_client), \
             patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=50), \
             patch("data_assets.assets.sonarqube.projects._SAFE_LIMIT", 50), \
             caplog.at_level(logging.WARNING):
            rows = asset.extract(engine, "tmp", make_ctx())

        # Collected 50 of 52 expected (3.8% shortfall) — within 5% tolerance
        assert rows == 50
        assert any("within 5%" in r.message for r in caplog.records)

    def test_reconciliation_accepts_surplus(self, sonarqube_env, caplog):
        """No warning when collected count >= initial total."""
        asset = self._make_asset()

        def mock_request(spec):
            params = spec.params
            if params.get("ps") == 1 and "q" not in params:
                return _make_probe_response(total=3)
            if params.get("ps") == 1:
                if params.get("q") in ("aa", "ab"):
                    return _make_probe_response(total=2)
                return _make_probe_response(total=0)
            q = params.get("q", "xx")
            return _make_page_response(
                [(f"{q}-1", f"Name {q} 1"), (f"{q}-2", f"Name {q} 2")], total=2,
            )

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        with patch.object(asset, "_create_client", return_value=mock_client), \
             patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=2), \
             patch("data_assets.assets.sonarqube.projects._SAFE_LIMIT", 2), \
             caplog.at_level(logging.WARNING):
            rows = asset.extract(engine, "tmp", make_ctx())

        assert rows >= 3
        warning_msgs = [r.message for r in caplog.records if r.levelno >= logging.WARNING]
        assert not any("expected" in m for m in warning_msgs)

    # -- max_pages safety limit ----------------------------------------

    def test_max_pages_safety_limit_stops_runaway_pagination(self, sonarqube_env, caplog):
        """The pagination loop stops at max_pages even if API always says has_more."""
        asset = self._make_asset()
        seen_keys: set[str] = set()
        pages_fetched = []

        def mock_request(spec):
            page = spec.params.get("p", 1)
            pages_fetched.append(page)
            return _make_page_response(
                [(f"k-{page}", f"Name {page}")],
                page_index=page, page_size=100, total=999_999,
            )

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        with patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=1), \
             caplog.at_level(logging.WARNING):
            asset._paginate_shard(
                mock_client, engine, "tmp", make_ctx(),
                q_param="ab", seen_keys=seen_keys,
            )

        from data_assets.assets.sonarqube.projects import _SAFE_LIMIT
        expected_max = (_SAFE_LIMIT // asset.pagination_config.page_size) + 1
        assert len(pages_fetched) == expected_max
        assert any("max_pages limit" in r.message for r in caplog.records)

    # -- max_depth safety limit ----------------------------------------

    def test_max_depth_guard_stops_runaway_recursion(self, sonarqube_env, caplog):
        """_extend_shard stops recursing at _MAX_SHARD_DEPTH."""
        asset = self._make_asset()
        seen_keys: set[str] = set()

        def mock_request(spec):
            # Every probe returns > _SAFE_LIMIT to force recursion at every level
            if spec.params.get("ps") == 1:
                return _make_probe_response(total=10_000)
            return _make_page_response([], total=0)

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        with patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=0), \
             caplog.at_level(logging.WARNING):
            asset._extend_shard(
                mock_client, engine, "tmp", make_ctx(),
                parent_prefix="ab", parent_total=10_000, seen_keys=seen_keys,
            )

        assert any("reached max depth" in r.message for r in caplog.records)

    # -- reconciliation shortfall guard --------------------------------

    def test_shortfall_over_5pct_raises(self, sonarqube_env):
        """Extraction aborts when sharding finds far fewer projects than expected."""
        import pytest

        asset = self._make_asset()

        def mock_request(spec):
            params = spec.params
            if params.get("ps") == 1 and "q" not in params:
                return _make_probe_response(total=100)
            if params.get("ps") == 1:
                if params.get("q") == "aa":
                    return _make_probe_response(total=1)
                return _make_probe_response(total=0)
            return _make_page_response([("only-one", "Only One")], total=1)

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request
        engine = MagicMock()

        with patch.object(asset, "_create_client", return_value=mock_client), \
             patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=1), \
             patch("data_assets.assets.sonarqube.projects._SAFE_LIMIT", 2), \
             pytest.raises(ValueError, match="shortfall"):
            asset.extract(engine, "tmp", make_ctx())

    # -- max_pages developer override ----------------------------------

    def test_max_pages_limits_simple_path(self, sonarqube_env):
        """max_pages=1 via context.params stops after 1 page on the simple path."""
        asset = self._make_asset()

        mock_client = MagicMock()
        # Probe: 3 total projects (below _SAFE_LIMIT)
        mock_client.request.side_effect = [
            _make_probe_response(total=3),
            # Page 1 response: 3 projects, total=3 → has_more=False
            _make_page_response([("k1", "N1"), ("k2", "N2"), ("k3", "N3")], total=3),
        ]

        ctx = make_ctx(params={"max_pages": 1})

        with patch.object(asset, "_create_client", return_value=mock_client), \
             patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=3):
            rows = asset.extract(MagicMock(), "tmp", ctx)

        assert rows == 3
        # Only the probe + page 1 should be called (max_pages=1 caps at 1 page)
        assert mock_client.request.call_count == 2

    def test_max_pages_skips_reconciliation_on_sharded_path(self, sonarqube_env):
        """max_pages override on the sharded path skips reconciliation check."""
        asset = self._make_asset()

        def mock_request(spec):
            params = spec.params
            if params.get("ps") == 1 and "q" not in params:
                return _make_probe_response(total=10_500)  # triggers sharding
            if params.get("ps") == 1:
                if params.get("q") == "aa":
                    return _make_probe_response(total=2)
                return _make_probe_response(total=0)
            return _make_page_response([("aa-1", "N1"), ("aa-2", "N2")], total=2)

        mock_client = MagicMock()
        mock_client.request.side_effect = mock_request

        # max_pages=1 — would normally fail reconciliation (collected 2 of 10,500)
        ctx = make_ctx(params={"max_pages": 1})

        with patch.object(asset, "_create_client", return_value=mock_client), \
             patch("data_assets.assets.sonarqube.projects.write_to_temp", return_value=2):
            # Should NOT raise ValueError about shortfall
            rows = asset.extract(MagicMock(), "tmp", ctx)

        assert rows == 2  # just the 'aa' shard's 2 projects


# ---------------------------------------------------------------------------
# SonarQubeBranches (entity-parallel, /api/project_branches/list)
# ---------------------------------------------------------------------------


class TestSonarQubeBranches:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.branches import SonarQubeBranches

        spec = SonarQubeBranches().build_entity_request("proj-alpha", make_ctx())
        assert spec.url == "https://sonar.test/api/project_branches/list"
        assert spec.params["project"] == "proj-alpha"

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.branches import SonarQubeBranches

        data = json.loads((FIXTURES / "branches_proj_alpha.json").read_text())
        df, state = SonarQubeBranches().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["name"] == "main"
        assert df.iloc[0]["is_main"] == True  # noqa: E712 (numpy bool)
        assert df.iloc[0]["quality_gate_status"] == "OK"
        assert df.iloc[1]["name"] == "develop"
        assert df.iloc[1]["is_main"] == False  # noqa: E712
        assert not state.has_more

    def test_parse_response_empty(self, sonarqube_env):
        from data_assets.assets.sonarqube.branches import SonarQubeBranches

        df, state = SonarQubeBranches().parse_response({"branches": []})
        assert len(df) == 0
        assert "project_key" in df.columns
        assert "name" in df.columns

    def test_entity_parallel_config(self, sonarqube_env):
        from data_assets.assets.sonarqube.branches import SonarQubeBranches
        from data_assets.core.enums import ParallelMode

        asset = SonarQubeBranches()
        assert asset.parallel_mode == ParallelMode.ENTITY_PARALLEL
        assert asset.parent_asset_name == "sonarqube_projects"
        assert asset.entity_key_column == "project_key"
        assert asset.primary_key == ["project_key", "name"]


# ---------------------------------------------------------------------------
# SonarQubeProjectDetails (entity-parallel, /api/components/show)
# ---------------------------------------------------------------------------


class TestSonarQubeProjectDetails:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.project_details import SonarQubeProjectDetails

        spec = SonarQubeProjectDetails().build_entity_request("proj-alpha", make_ctx())
        assert spec.url == "https://sonar.test/api/components/show"
        assert spec.params["component"] == "proj-alpha"

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.project_details import SonarQubeProjectDetails

        data = json.loads((FIXTURES / "component_show_proj_alpha.json").read_text())
        df, state = SonarQubeProjectDetails().parse_response(data)
        assert len(df) == 1
        assert df.iloc[0]["key"] == "proj-alpha"
        assert df.iloc[0]["description"] == "Main backend service"
        assert df.iloc[0]["visibility"] == "public"
        assert df.iloc[0]["version"] == "2.1.0"
        assert json.loads(df.iloc[0]["tags"]) == ["backend", "production"]
        assert not state.has_more

    def test_parse_response_empty(self, sonarqube_env):
        from data_assets.assets.sonarqube.project_details import SonarQubeProjectDetails

        df, state = SonarQubeProjectDetails().parse_response({"component": {}})
        assert len(df) == 0
        assert "key" in df.columns

    def test_entity_parallel_config(self, sonarqube_env):
        from data_assets.assets.sonarqube.project_details import SonarQubeProjectDetails
        from data_assets.core.enums import ParallelMode

        asset = SonarQubeProjectDetails()
        assert asset.parallel_mode == ParallelMode.ENTITY_PARALLEL
        assert asset.parent_asset_name == "sonarqube_projects"
        assert asset.primary_key == ["key"]


# ---------------------------------------------------------------------------
# SonarQubeAnalyses (entity-parallel, /api/project_analyses/search)
# ---------------------------------------------------------------------------


class TestSonarQubeAnalyses:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalyses

        spec = SonarQubeAnalyses().build_entity_request("proj-alpha", make_ctx())
        assert spec.url == "https://sonar.test/api/project_analyses/search"
        assert spec.params["project"] == "proj-alpha"
        assert spec.params["p"] == 1

    def test_build_entity_request_with_checkpoint(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalyses

        spec = SonarQubeAnalyses().build_entity_request(
            "proj-alpha", make_ctx(), checkpoint={"next_page": 3}
        )
        assert spec.params["p"] == 3

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalyses

        data = json.loads((FIXTURES / "analyses_proj_alpha.json").read_text())
        df, state = SonarQubeAnalyses().parse_response(data)
        assert len(df) == 2
        assert df.iloc[0]["key"] == "AXK1-analysis-001"
        assert df.iloc[0]["project_version"] == "2.1.0"
        assert df.iloc[0]["detected_ci"] == "GitHub Actions"
        assert "events" not in df.columns  # events dropped
        assert not state.has_more

    def test_parse_response_empty(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalyses

        resp = {"paging": {"pageIndex": 1, "pageSize": 100, "total": 0}, "analyses": []}
        df, state = SonarQubeAnalyses().parse_response(resp)
        assert len(df) == 0
        assert "key" in df.columns

    def test_entity_parallel_config(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalyses
        from data_assets.core.enums import ParallelMode

        asset = SonarQubeAnalyses()
        assert asset.parallel_mode == ParallelMode.ENTITY_PARALLEL
        assert asset.parent_asset_name == "sonarqube_projects"
        assert asset.entity_key_column == "project_key"
        assert asset.date_column == "date"


# ---------------------------------------------------------------------------
# SonarQubeAnalysisEvents (entity-parallel, flattened events)
# ---------------------------------------------------------------------------


class TestSonarQubeAnalysisEvents:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalysisEvents

        spec = SonarQubeAnalysisEvents().build_entity_request("proj-alpha", make_ctx())
        assert spec.url == "https://sonar.test/api/project_analyses/search"
        assert spec.params["project"] == "proj-alpha"

    def test_parse_response_extracts_events(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalysisEvents

        data = json.loads((FIXTURES / "analyses_proj_alpha.json").read_text())
        df, state = SonarQubeAnalysisEvents().parse_response(data)
        # Only analysis-001 has events; analysis-002 has empty events
        assert len(df) == 1
        assert df.iloc[0]["key"] == "EVT-001"
        assert df.iloc[0]["analysis_key"] == "AXK1-analysis-001"
        assert df.iloc[0]["category"] == "QUALITY_GATE"
        details = json.loads(df.iloc[0]["details"])
        assert details["qualityGate"]["status"] == "OK"

    def test_parse_response_no_events(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalysisEvents

        resp = {
            "paging": {"pageIndex": 1, "pageSize": 100, "total": 1},
            "analyses": [{"key": "A1", "date": "2025-01-01", "events": []}],
        }
        df, state = SonarQubeAnalysisEvents().parse_response(resp)
        assert len(df) == 0
        assert "key" in df.columns

    def test_entity_parallel_config(self, sonarqube_env):
        from data_assets.assets.sonarqube.analyses import SonarQubeAnalysisEvents
        from data_assets.core.enums import ParallelMode

        asset = SonarQubeAnalysisEvents()
        assert asset.parallel_mode == ParallelMode.ENTITY_PARALLEL
        assert asset.parent_asset_name == "sonarqube_projects"
        assert asset.primary_key == ["key"]


# ---------------------------------------------------------------------------
# SonarQubeMeasuresHistory (entity-parallel, /api/measures/search_history)
# ---------------------------------------------------------------------------


class TestSonarQubeMeasuresHistory:
    def test_build_entity_request(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        entity_key = {"project_key": "proj-alpha", "name": "main"}
        spec = SonarQubeMeasuresHistory().build_entity_request(entity_key, make_ctx())
        assert spec.url == "https://sonar.test/api/measures/search_history"
        assert spec.params["component"] == "proj-alpha"
        assert spec.params["branch"] == "main"
        assert "coverage" in spec.params["metrics"]
        assert "new_coverage" not in spec.params["metrics"]  # new_* excluded from history
        assert spec.params["p"] == 1
        assert "from" in spec.params
        assert "to" in spec.params

    def test_build_entity_request_with_start_date(self, sonarqube_env):
        from datetime import UTC, date, datetime, timedelta

        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        entity_key = {"project_key": "proj-alpha", "name": "develop"}
        ctx = make_ctx(start_date=datetime(2025, 4, 1, tzinfo=UTC))
        spec = SonarQubeMeasuresHistory().build_entity_request(entity_key, ctx)
        # from = max(today - 720, start_date) — start_date wins when more recent
        expected_from = max(date.today() - timedelta(days=720), date(2025, 4, 1)).isoformat()
        assert spec.params["from"] == expected_from

    def test_build_entity_request_from_to_dates(self, sonarqube_env):
        from datetime import date, timedelta

        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        entity_key = {"project_key": "proj-alpha", "name": "main"}
        spec = SonarQubeMeasuresHistory().build_entity_request(entity_key, make_ctx())
        expected_from = (date.today() - timedelta(days=720)).isoformat()
        assert spec.params["from"] == expected_from
        assert spec.params["to"] == date.today().isoformat()

    def test_parse_response(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        data = json.loads((FIXTURES / "measures_history_proj_alpha.json").read_text())
        df, state = SonarQubeMeasuresHistory().parse_response(data)
        # 2 metrics × 2 dates = 4 rows
        assert len(df) == 4
        assert set(df["metric"]) == {"coverage", "bugs"}
        assert df[df["metric"] == "coverage"].iloc[0]["value"] == "85.5"
        assert not state.has_more

    def test_parse_response_has_collected_at(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        data = json.loads((FIXTURES / "measures_history_proj_alpha.json").read_text())
        df, _ = SonarQubeMeasuresHistory().parse_response(data)
        assert "collected_at" in df.columns
        assert all(pd.notna(df["collected_at"]))

    def test_parse_response_empty(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        resp = {"paging": {"pageIndex": 1, "pageSize": 100, "total": 0}, "measures": []}
        df, state = SonarQubeMeasuresHistory().parse_response(resp)
        assert len(df) == 0
        assert "metric" in df.columns
        assert "date" in df.columns
        assert "branch" in df.columns

    def test_entity_parallel_config(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory
        from data_assets.core.enums import ParallelMode

        asset = SonarQubeMeasuresHistory()
        assert asset.parallel_mode == ParallelMode.ENTITY_PARALLEL
        assert asset.parent_asset_name == "sonarqube_branches"
        assert asset.entity_key_column is None
        assert asset.entity_key_map == {"project_key": "project_key", "name": "branch"}
        assert asset.primary_key == ["project_key", "branch", "metric", "date"]
        assert asset.date_column == "date"

    def test_history_days_back_default(self, sonarqube_env):
        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        assert SonarQubeMeasuresHistory().history_days_back == 720

    def test_history_days_back_from_env(self, sonarqube_env, monkeypatch):
        from data_assets.assets.sonarqube.measures_history import SonarQubeMeasuresHistory

        monkeypatch.setenv("SONARQUBE_HISTORY_DAYS_BACK", "180")
        assert SonarQubeMeasuresHistory().history_days_back == 180
