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
