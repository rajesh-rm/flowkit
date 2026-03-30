"""Tests for loader DDL generation (string logic, no DB)."""

from data_assets.core.column import Column
from data_assets.load.loader import _column_ddl


def test_column_ddl_basic():
    col = Column("name", "TEXT")
    assert _column_ddl(col) == '"name" TEXT'


def test_column_ddl_not_null():
    col = Column("id", "INTEGER", nullable=False)
    assert _column_ddl(col) == '"id" INTEGER NOT NULL'


def test_column_ddl_with_default():
    col = Column("created_at", "TIMESTAMPTZ", nullable=False, default="now()")
    assert _column_ddl(col) == '"created_at" TIMESTAMPTZ NOT NULL DEFAULT now()'


def test_column_ddl_nullable_with_default():
    col = Column("score", "FLOAT", nullable=True, default="0.0")
    assert _column_ddl(col) == '"score" FLOAT DEFAULT 0.0'
