"""Unit tests for dialect-specific methods (no DB required)."""

from __future__ import annotations

import pandas as pd
import pytest
from sqlalchemy import DateTime, Integer, String, Text

from data_assets.core.column import Column
from data_assets.db.dialect import MariaDBDialect, PostgresDialect


# ---------------------------------------------------------------------------
# Column DDL generation
# ---------------------------------------------------------------------------


class TestColumnDDL:
    _pg = PostgresDialect()
    _maria = MariaDBDialect()

    # -- PostgreSQL --

    def test_pg_basic(self):
        assert self._pg.column_ddl(Column("x", Text())) == '"x" TEXT'

    def test_pg_not_null(self):
        assert self._pg.column_ddl(Column("id", Integer(), nullable=False)) == '"id" INTEGER NOT NULL'

    def test_pg_with_default(self):
        col = Column("ts", DateTime(timezone=True), nullable=False, default="now()")
        ddl = self._pg.column_ddl(col)
        assert ddl.startswith('"ts" TIMESTAMP')
        assert "NOT NULL" in ddl
        assert "DEFAULT now()" in ddl

    # -- MariaDB --

    def test_mariadb_basic(self):
        assert self._maria.column_ddl(Column("x", Text())) == "`x` TEXT"

    def test_mariadb_not_null(self):
        assert self._maria.column_ddl(Column("id", Integer(), nullable=False)) == "`id` INTEGER NOT NULL"

    def test_mariadb_with_default(self):
        col = Column("ts", DateTime(timezone=True), nullable=False, default="now()")
        ddl = self._maria.column_ddl(col)
        assert ddl.startswith("`ts` DATETIME")
        assert "NOT NULL" in ddl
        assert "DEFAULT now()" in ddl


# ---------------------------------------------------------------------------
# MariaDB-specific methods
# ---------------------------------------------------------------------------


class TestMariaDBDialect:
    _maria = MariaDBDialect()

    # -- adjust_pk_columns --

    def test_adjust_pk_columns_converts_text_to_varchar(self):
        cols = [Column("id", Text()), Column("name", Text())]
        result = self._maria.adjust_pk_columns(cols, {"id"})
        assert isinstance(result[0].sa_type, String)
        assert result[0].sa_type.length == 255
        # Non-PK TEXT column untouched
        assert isinstance(result[1].sa_type, Text)

    def test_adjust_pk_columns_leaves_non_text_unchanged(self):
        cols = [Column("id", String(100))]
        result = self._maria.adjust_pk_columns(cols, {"id"})
        assert isinstance(result[0].sa_type, String)
        assert result[0].sa_type.length == 100

    def test_adjust_pk_columns_empty_pk_set(self):
        cols = [Column("name", Text())]
        result = self._maria.adjust_pk_columns(cols, set())
        assert isinstance(result[0].sa_type, Text)

    # -- prepare_dataframe --

    def test_prepare_dataframe_strips_tz(self):
        df = pd.DataFrame({"ts": pd.to_datetime(["2025-01-01"], utc=True)})
        df = df.copy()
        result = self._maria.prepare_dataframe(df)
        assert result["ts"].dt.tz is None

    def test_prepare_dataframe_leaves_naive_datetimes(self):
        df = pd.DataFrame({"ts": pd.to_datetime(["2025-01-01"])})
        df = df.copy()
        result = self._maria.prepare_dataframe(df)
        assert result["ts"].dt.tz is None

    def test_prepare_dataframe_leaves_non_datetime_columns(self):
        df = pd.DataFrame({"name": ["Alice", "Bob"]})
        df = df.copy()
        result = self._maria.prepare_dataframe(df)
        assert list(result["name"]) == ["Alice", "Bob"]

    def test_prepare_dataframe_mixed_columns(self):
        """Verify correct column targeting with heterogeneous types."""
        df = pd.DataFrame({
            "aware_ts": pd.to_datetime(["2025-06-01"], utc=True),
            "naive_ts": pd.to_datetime(["2025-06-01"]),
            "count": [42],
            "label": ["prod"],
        })
        df = df.copy()
        result = self._maria.prepare_dataframe(df)
        # tz-aware column stripped
        assert result["aware_ts"].dt.tz is None
        # naive datetime unchanged
        assert result["naive_ts"].dt.tz is None
        # non-datetime columns untouched
        assert result["count"].iloc[0] == 42
        assert result["label"].iloc[0] == "prod"


# ---------------------------------------------------------------------------
# PostgresDialect base-class defaults
# ---------------------------------------------------------------------------


class TestPostgresDialect:
    _pg = PostgresDialect()

    def test_adjust_pk_columns_preserves_text(self):
        """Postgres can use TEXT in primary keys — columns should pass through unchanged."""
        cols = [Column("id", Text()), Column("name", Text())]
        result = self._pg.adjust_pk_columns(cols, {"id"})
        assert isinstance(result[0].sa_type, Text)
        assert isinstance(result[1].sa_type, Text)

    def test_prepare_dataframe_preserves_tz(self):
        """Postgres TIMESTAMPTZ handles tz-aware datetimes — no stripping."""
        df = pd.DataFrame({"ts": pd.to_datetime(["2025-01-01"], utc=True)})
        df = df.copy()
        result = self._pg.prepare_dataframe(df)
        assert result["ts"].dt.tz is not None


# ---------------------------------------------------------------------------
# SQL expression helpers (for TransformAsset queries)
# ---------------------------------------------------------------------------


class TestSqlExpressionHelpers:
    _pg = PostgresDialect()
    _maria = MariaDBDialect()

    # -- week_start_from_ts --

    def test_pg_week_start_from_ts(self):
        assert self._pg.week_start_from_ts("analysis_date") == (
            "DATE_TRUNC('week', (analysis_date) AT TIME ZONE 'UTC')::date"
        )

    def test_maria_week_start_from_ts(self):
        assert self._maria.week_start_from_ts("analysis_date") == (
            "DATE_SUB(DATE(analysis_date), INTERVAL WEEKDAY(analysis_date) DAY)"
        )

    def test_pg_week_start_from_ts_wraps_complex_expr(self):
        """Parens around the expression must be emitted so compound exprs parse."""
        fragment = self._pg.week_start_from_ts("MIN(analysis_date)")
        assert "(MIN(analysis_date))" in fragment

    def test_maria_week_start_from_ts_wraps_complex_expr(self):
        fragment = self._maria.week_start_from_ts("MIN(analysis_date)")
        assert "DATE(MIN(analysis_date))" in fragment
        assert "WEEKDAY(MIN(analysis_date))" in fragment

    # -- date_add_days --

    def test_pg_date_add_days_negative(self):
        assert self._pg.date_add_days("CURRENT_DATE", -7) == (
            "((CURRENT_DATE) + INTERVAL '-7 days')::date"
        )

    def test_pg_date_add_days_positive(self):
        assert self._pg.date_add_days("week_start_date", 7) == (
            "((week_start_date) + INTERVAL '7 days')::date"
        )

    def test_maria_date_add_days_negative(self):
        assert self._maria.date_add_days("CURRENT_DATE", -7) == (
            "DATE_ADD(CURRENT_DATE, INTERVAL -7 DAY)"
        )

    def test_maria_date_add_days_positive(self):
        assert self._maria.date_add_days("week_start_date", 7) == (
            "DATE_ADD(week_start_date, INTERVAL 7 DAY)"
        )

    # -- cast_bigint --

    def test_pg_cast_bigint(self):
        assert self._pg.cast_bigint("x") == "CAST(x AS BIGINT)"

    def test_maria_cast_bigint(self):
        """MariaDB uses SIGNED (not BIGINT) for 64-bit signed integer casts."""
        assert self._maria.cast_bigint("x") == "CAST(x AS SIGNED)"

    def test_pg_cast_bigint_window_expr(self):
        expr = "SUM(new_projects) OVER (ORDER BY week_start_date)"
        assert self._pg.cast_bigint(expr) == f"CAST({expr} AS BIGINT)"

    def test_maria_cast_bigint_window_expr(self):
        expr = "SUM(new_projects) OVER (ORDER BY week_start_date)"
        assert self._maria.cast_bigint(expr) == f"CAST({expr} AS SIGNED)"
