"""Unit tests for sonarqube_adoption_trend transform asset."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import BigInteger, Date

from data_assets.assets.transforms.sonarqube_adoption_trend import (
    SonarqubeAdoptionTrend,
)
from data_assets.core.enums import AssetType, LoadStrategy, RunMode
from data_assets.core.registry import all_assets, discover
from data_assets.core.run_context import RunContext
from data_assets.core.transform_asset import TransformAsset
from data_assets.db.dialect import MariaDBDialect, PostgresDialect


_CTX = RunContext(
    run_id=UUID(int=0),
    mode=RunMode.TRANSFORM,
    asset_name="sonarqube_adoption_trend",
)


class TestRegistration:

    def test_registered_under_canonical_name(self):
        discover()
        assert "sonarqube_adoption_trend" in all_assets()
        assert all_assets()["sonarqube_adoption_trend"] is SonarqubeAdoptionTrend

    def test_is_transform_asset(self):
        asset = SonarqubeAdoptionTrend()
        assert isinstance(asset, TransformAsset)
        assert asset.asset_type == AssetType.TRANSFORM


class TestClassAttributes:
    _asset = SonarqubeAdoptionTrend()

    def test_target(self):
        assert self._asset.target_schema == "mart"
        assert self._asset.target_table == "sonarqube_adoption_trend"

    def test_source_tables(self):
        assert self._asset.source_tables == ["sonarqube_measures_history"]

    def test_run_mode_and_load_strategy(self):
        assert self._asset.default_run_mode == RunMode.TRANSFORM
        assert self._asset.load_strategy == LoadStrategy.FULL_REPLACE

    def test_primary_key(self):
        assert self._asset.primary_key == ["week_start_date"]

    def test_indexes_declared(self):
        assert len(self._asset.indexes) == 1
        assert self._asset.indexes[0].columns == ("week_start_date",)

    def test_query_timeout(self):
        assert self._asset.query_timeout_seconds == 300


class TestColumns:
    _asset = SonarqubeAdoptionTrend()

    def test_three_columns(self):
        assert [c.name for c in self._asset.columns] == [
            "week_start_date", "new_projects", "cumulative_projects",
        ]

    def test_week_start_date_is_date_not_null(self):
        col = self._asset.columns[0]
        assert isinstance(col.sa_type, Date)
        assert col.nullable is False

    def test_counts_are_bigint_not_null(self):
        for col in self._asset.columns[1:]:
            assert isinstance(col.sa_type, BigInteger), col.name
            assert col.nullable is False, col.name


class TestQueryRendering:
    _asset = SonarqubeAdoptionTrend()

    # -- shared structural invariants --

    def test_postgres_renders_recursive_cte(self):
        sql = self._asset.query(_CTX, PostgresDialect())
        assert "WITH RECURSIVE" in sql

    def test_mariadb_renders_recursive_cte(self):
        sql = self._asset.query(_CTX, MariaDBDialect())
        assert "WITH RECURSIVE" in sql

    def test_references_source_table_fully_qualified(self):
        for d in (PostgresDialect(), MariaDBDialect()):
            sql = self._asset.query(_CTX, d)
            assert "raw.sonarqube_measures_history" in sql

    def test_clock_skew_guard_present(self):
        for d in (PostgresDialect(), MariaDBDialect()):
            sql = self._asset.query(_CTX, d)
            assert "analysis_date <= NOW()" in sql

    def test_in_progress_week_cutoff_present(self):
        for d in (PostgresDialect(), MariaDBDialect()):
            sql = self._asset.query(_CTX, d)
            assert "onboarded_week <= (SELECT wk FROM last_completed)" in sql

    def test_groups_by_project_key_only(self):
        """Multi-branch projects must count once — GROUP BY project_key, not branch."""
        for d in (PostgresDialect(), MariaDBDialect()):
            sql = self._asset.query(_CTX, d)
            assert "GROUP BY project_key" in sql
            assert "GROUP BY project_key, branch" not in sql

    # -- dialect-specific fragments --

    def test_postgres_specific_fragments(self):
        sql = self._asset.query(_CTX, PostgresDialect())
        assert "DATE_TRUNC('week'" in sql
        assert "AT TIME ZONE 'UTC'" in sql
        assert "CAST(" in sql and "AS BIGINT)" in sql
        assert "INTERVAL '7 days'" in sql or "INTERVAL '-7 days'" in sql

    def test_mariadb_specific_fragments(self):
        sql = self._asset.query(_CTX, MariaDBDialect())
        assert "DATE_SUB" in sql
        assert "WEEKDAY(" in sql
        assert "DATE_ADD(" in sql
        assert "INTERVAL 7 DAY" in sql
        assert "INTERVAL -7 DAY" in sql
        assert "CAST(" in sql and "AS SIGNED)" in sql

    def test_postgres_does_not_contain_mariadb_syntax(self):
        sql = self._asset.query(_CTX, PostgresDialect())
        assert "WEEKDAY(" not in sql
        assert "AS SIGNED)" not in sql

    def test_mariadb_does_not_contain_postgres_syntax(self):
        sql = self._asset.query(_CTX, MariaDBDialect())
        assert "DATE_TRUNC" not in sql
        assert "AS BIGINT)" not in sql
        assert "AT TIME ZONE" not in sql

    # -- output alias order matches declared columns --

    def test_select_aliases_match_declared_columns(self):
        """Asset validation requires SELECT aliases in the exact declared order."""
        for d in (PostgresDialect(), MariaDBDialect()):
            sql = self._asset.query(_CTX, d)
            idx_week = sql.find("AS week_start_date")
            idx_new = sql.find("AS new_projects")
            idx_cum = sql.find("AS cumulative_projects")
            # All three aliases present
            assert idx_week != -1
            assert idx_new != -1
            assert idx_cum != -1
            # Final SELECT aliases in declared order (last occurrence of each)
            final_week = sql.rfind("AS week_start_date")
            final_new = sql.rfind("AS new_projects")
            final_cum = sql.rfind("AS cumulative_projects")
            assert final_week < final_new < final_cum
