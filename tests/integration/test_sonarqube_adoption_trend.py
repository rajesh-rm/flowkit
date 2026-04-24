"""End-to-end test for sonarqube_adoption_trend transform.

Seeds raw.sonarqube_measures_history with a multi-week, multi-branch,
multi-metric dataset, runs the transform, and asserts the output shape.
Runs against both Postgres and MariaDB automatically — the `db_engine`
fixture in tests/conftest.py is parametrised over both backends.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from data_assets.core.registry import discover, get
from data_assets.db.dialect import get_dialect
from data_assets.load.loader import create_table
from tests.integration._db_utils import read_rows


def _last_completed_monday(today_utc: datetime | None = None) -> datetime:
    """Return the Monday of the ISO week that ended before today (UTC)."""
    today_utc = today_utc or datetime.now(timezone.utc)
    today_date = today_utc.date()
    prior = today_date - timedelta(days=7)
    # weekday(): 0=Mon..6=Sun
    monday = prior - timedelta(days=prior.weekday())
    return datetime(monday.year, monday.month, monday.day, tzinfo=timezone.utc)


def _seed_measures_history(engine, rows: list[dict]) -> None:
    """Create raw.sonarqube_measures_history (dialect-correctly) and insert rows."""
    discover()
    asset = get("sonarqube_measures_history")()
    create_table(
        engine, "raw", asset.target_table, asset.columns,
        primary_key=asset.primary_key,
    )
    df = pd.DataFrame(rows)
    df = get_dialect(engine).prepare_dataframe(df.copy())
    df.to_sql(
        asset.target_table, engine, schema="raw",
        if_exists="append", index=False,
    )


@pytest.mark.integration
class TestSonarqubeAdoptionTrendE2E:

    def test_weekly_onboarding_trend_with_gap_and_multi_branch(self, run_engine):
        # Week anchors relative to current UTC date.
        w4 = _last_completed_monday()
        w3 = w4 - timedelta(days=7)
        w2 = w4 - timedelta(days=14)
        w1 = w4 - timedelta(days=21)

        # Project A — multi-branch × multi-metric, onboarded in W1.
        # Also includes later analyses to verify MIN() picks the earliest.
        rows: list[dict] = []
        project_a_first = w1 + timedelta(days=2)  # Wed of W1
        for branch in ("main", "develop"):
            for metric in ("ncloc", "coverage", "bugs"):
                rows.append({
                    "project_key": "proj-alpha",
                    "branch": branch,
                    "metric_key": metric,
                    "analysis_date": project_a_first,
                    "value": "42",
                    "collected_at": project_a_first,
                })
        # Later analysis for A — must NOT change onboarding week.
        rows.append({
            "project_key": "proj-alpha",
            "branch": "main",
            "metric_key": "ncloc",
            "analysis_date": w3 + timedelta(days=1),
            "value": "84",
            "collected_at": w3 + timedelta(days=1),
        })

        # Projects B and C — both onboarded in W3 (two different days).
        rows.append({
            "project_key": "proj-beta",
            "branch": "main",
            "metric_key": "ncloc",
            "analysis_date": w3 + timedelta(days=2),
            "value": "10",
            "collected_at": w3 + timedelta(days=2),
        })
        rows.append({
            "project_key": "proj-gamma",
            "branch": "main",
            "metric_key": "ncloc",
            "analysis_date": w3 + timedelta(days=3),
            "value": "20",
            "collected_at": w3 + timedelta(days=3),
        })

        _seed_measures_history(run_engine, rows)

        from data_assets.runner import run_asset
        result = run_asset("sonarqube_adoption_trend", run_mode="transform")

        assert result["status"] == "success", result
        assert result["rows_loaded"] == 4, result

        df = read_rows(
            run_engine, "mart", "sonarqube_adoption_trend",
            order_by=["week_start_date"],
        )
        df = df[["week_start_date", "new_projects", "cumulative_projects"]]
        assert len(df) == 4

        # Gap-free spine: W1 → W2 → W3 → W4
        week_dates = [pd.to_datetime(d).date() for d in df["week_start_date"]]
        assert week_dates == [w1.date(), w2.date(), w3.date(), w4.date()]

        # Every week is a Monday (weekday == 0)
        for wd in week_dates:
            assert wd.weekday() == 0, f"{wd} is not a Monday"

        # new_projects: proj-alpha in W1, nothing in W2, beta+gamma in W3, none in W4
        assert list(df["new_projects"]) == [1, 0, 2, 0]

        # cumulative is monotonic non-decreasing
        assert list(df["cumulative_projects"]) == [1, 1, 3, 3]

        # run_history recorded as success, no locks left.
        history = read_rows(
            run_engine, "data_ops", "run_history",
            where={"asset_name": "sonarqube_adoption_trend"},
        )
        assert len(history) == 1
        assert history.iloc[0]["status"] == "success"

        locks = read_rows(
            run_engine, "data_ops", "run_locks",
            where={"asset_name": "sonarqube_adoption_trend"},
        )
        assert len(locks) == 0

    def test_future_dated_analysis_excluded_by_clock_skew_guard(self, run_engine):
        """The `WHERE analysis_date <= NOW()` guard in the asset SQL must
        exclude future-dated rows (clock skew / test contamination) so they
        cannot leak onto the dashboard as a phantom "new project" in a week
        that has not happened yet.
        """
        w4 = _last_completed_monday()
        w3 = w4 - timedelta(days=7)
        w2 = w4 - timedelta(days=14)
        w1 = w4 - timedelta(days=21)

        now_utc = datetime.now(timezone.utc)

        rows: list[dict] = [
            # A legitimate historical onboarding in W1.
            {
                "project_key": "proj-alpha",
                "branch": "main",
                "metric_key": "ncloc",
                "analysis_date": w1 + timedelta(days=2),
                "value": "42",
                "collected_at": w1 + timedelta(days=2),
            },
            # A future-dated row for a different project — 30 days ahead.
            # The guard must drop this; if it doesn't, a phantom week shows
            # up and proj-future appears as a new onboarding.
            {
                "project_key": "proj-future",
                "branch": "main",
                "metric_key": "ncloc",
                "analysis_date": now_utc + timedelta(days=30),
                "value": "99",
                "collected_at": now_utc,
            },
        ]

        _seed_measures_history(run_engine, rows)

        from data_assets.runner import run_asset
        result = run_asset("sonarqube_adoption_trend", run_mode="transform")

        assert result["status"] == "success", result

        df = read_rows(
            run_engine, "mart", "sonarqube_adoption_trend",
            order_by=["week_start_date"],
        )
        df = df[["week_start_date", "new_projects", "cumulative_projects"]]

        # Exactly 4 rows: W1 (1 onboarding), W2/W3/W4 (zero).
        # Future-dated proj-future must NOT create a 5th row beyond W4.
        assert len(df) == 4, (
            f"Future-dated analysis_date leaked past the <= NOW() guard. "
            f"Got {len(df)} rows: {df.to_dict('records')}"
        )
        week_dates = [pd.to_datetime(d).date() for d in df["week_start_date"]]
        assert week_dates == [w1.date(), w2.date(), w3.date(), w4.date()]

        # Only proj-alpha counted in W1; the total is 1, not 2.
        assert list(df["new_projects"]) == [1, 0, 0, 0]
        assert list(df["cumulative_projects"]) == [1, 1, 1, 1]
