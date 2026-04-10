"""Integration tests for loader: DDL, temp tables, and promotion against real Postgres."""

from __future__ import annotations

import uuid

import pandas as pd
import pytest
from sqlalchemy import DateTime, Float, Integer, Text, inspect, text

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy
from data_assets.load.loader import (
    _column_ddl,
    create_table,
    create_temp_table,
    drop_temp_table,
    ensure_columns,
    ensure_indexes,
    promote,
    read_temp_table,
    temp_table_exists,
    temp_table_name,
    write_to_temp,
)

COLS = [
    Column("id", Integer(), nullable=False),
    Column("name", Text()),
    Column("score", Float(), nullable=True),
]
PK = ["id"]


# ---------------------------------------------------------------------------
# DDL string generation (no DB needed)
# ---------------------------------------------------------------------------


class TestColumnDDL:
    def test_basic(self):
        assert _column_ddl(Column("x", Text())) == '"x" TEXT'

    def test_not_null(self):
        assert _column_ddl(Column("id", Integer(), nullable=False)) == '"id" INTEGER NOT NULL'

    def test_with_default(self):
        col = Column("ts", DateTime(timezone=True), nullable=False, default="now()")
        ddl = _column_ddl(col)
        assert ddl.startswith('"ts" TIMESTAMP')
        assert "NOT NULL" in ddl
        assert "DEFAULT now()" in ddl


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestCreateTable:
    def test_creates_table(self, clean_db):
        create_table(clean_db, "raw", "test_create", COLS, PK)
        assert inspect(clean_db).has_table("test_create", schema="raw")

    def test_idempotent(self, clean_db):
        create_table(clean_db, "raw", "test_idem", COLS, PK)
        create_table(clean_db, "raw", "test_idem", COLS, PK)  # no error
        assert inspect(clean_db).has_table("test_idem", schema="raw")

    def test_unlogged(self, clean_db):
        create_table(clean_db, "temp_store", "test_unlog", COLS, unlogged=True)
        assert inspect(clean_db).has_table("test_unlog", schema="temp_store")


# ---------------------------------------------------------------------------
# Schema contracts (ensure_columns)
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestEnsureColumns:
    def test_evolve_adds_column(self, clean_db):
        create_table(clean_db, "raw", "test_evolve", COLS[:2], PK)
        ensure_columns(clean_db, "raw", "test_evolve", COLS, schema_contract="evolve")
        col_names = {c["name"] for c in inspect(clean_db).get_columns("test_evolve", schema="raw")}
        assert "score" in col_names

    def test_freeze_raises_on_new_column(self, clean_db):
        create_table(clean_db, "raw", "test_freeze", COLS[:2], PK)
        with pytest.raises(ValueError, match="freeze"):
            ensure_columns(clean_db, "raw", "test_freeze", COLS, schema_contract="freeze")

    def test_discard_ignores_new_column(self, clean_db):
        create_table(clean_db, "raw", "test_discard", COLS[:2], PK)
        ensure_columns(clean_db, "raw", "test_discard", COLS, schema_contract="discard")
        col_names = {c["name"] for c in inspect(clean_db).get_columns("test_discard", schema="raw")}
        assert "score" not in col_names

    def test_no_op_when_columns_match(self, clean_db):
        create_table(clean_db, "raw", "test_match", COLS, PK)
        ensure_columns(clean_db, "raw", "test_match", COLS)  # no error, no changes


# ---------------------------------------------------------------------------
# Temp table lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestTempTables:
    def test_name_generation(self):
        run_id = uuid.uuid4()
        name = temp_table_name("my_asset", run_id)
        assert name.startswith("my_asset_")
        assert len(name) > len("my_asset_")

    def test_create_write_read_drop(self, clean_db):
        run_id = uuid.uuid4()
        tname = create_temp_table(clean_db, "test_asset", run_id, COLS)
        assert temp_table_exists(clean_db, tname)

        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "score": [1.0, 2.0]})
        rows = write_to_temp(clean_db, tname, df)
        assert rows == 2

        result = read_temp_table(clean_db, tname)
        assert len(result) == 2

        drop_temp_table(clean_db, tname)
        assert not temp_table_exists(clean_db, tname)

    def test_write_empty_df_returns_zero(self, clean_db):
        run_id = uuid.uuid4()
        tname = create_temp_table(clean_db, "test_empty", run_id, COLS)
        rows = write_to_temp(clean_db, tname, pd.DataFrame(columns=["id", "name", "score"]))
        assert rows == 0


# ---------------------------------------------------------------------------
# Promotion strategies
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPromote:
    def _setup(self, engine, run_id):
        """Create temp table with test data and return its name."""
        tname = create_temp_table(engine, "promo_test", run_id, COLS)
        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "score": [1.0, 2.0, 3.0]})
        write_to_temp(engine, tname, df)
        return tname

    def test_full_replace(self, clean_db):
        # Seed main table with existing data
        create_table(clean_db, "raw", "promo_fr", COLS, PK)
        with clean_db.begin() as conn:
            conn.execute(text(
                'INSERT INTO raw.promo_fr (id, name, score) VALUES (99, \'old\', 0.0)'
            ))

        tname = self._setup(clean_db, uuid.uuid4())
        rows = promote(clean_db, tname, "raw", "promo_fr", COLS, PK, LoadStrategy.FULL_REPLACE)
        assert rows == 3

        result = pd.read_sql("SELECT * FROM raw.promo_fr ORDER BY id", clean_db)
        assert len(result) == 3  # old row replaced
        assert 99 not in result["id"].values

    def test_upsert(self, clean_db):
        # Seed with overlapping data
        create_table(clean_db, "raw", "promo_up", COLS, PK)
        with clean_db.begin() as conn:
            conn.execute(text(
                'INSERT INTO raw.promo_up (id, name, score) VALUES (1, \'old\', 0.0), (99, \'keep\', 9.0)'
            ))

        tname = self._setup(clean_db, uuid.uuid4())
        rows = promote(clean_db, tname, "raw", "promo_up", COLS, PK, LoadStrategy.UPSERT)
        assert rows == 3

        result = pd.read_sql("SELECT * FROM raw.promo_up ORDER BY id", clean_db)
        assert len(result) == 4  # 3 upserted + 1 existing (id=99)
        assert result[result["id"] == 1].iloc[0]["name"] == "a"  # updated
        assert result[result["id"] == 99].iloc[0]["name"] == "keep"  # untouched

    def test_append(self, clean_db):
        # APPEND does plain INSERT with no conflict handling, so the target
        # must not have a PK constraint if duplicate keys are expected.
        create_table(clean_db, "raw", "promo_ap", COLS, primary_key=[])
        with clean_db.begin() as conn:
            conn.execute(text(
                'INSERT INTO raw.promo_ap (id, name, score) VALUES (1, \'existing\', 0.0)'
            ))

        tname = self._setup(clean_db, uuid.uuid4())
        rows = promote(clean_db, tname, "raw", "promo_ap", COLS, [], LoadStrategy.APPEND)
        assert rows == 3

        result = pd.read_sql("SELECT * FROM raw.promo_ap ORDER BY id", clean_db)
        # Append doesn't handle conflicts — both id=1 rows exist
        assert len(result) == 4

    def test_upsert_deduplicates_temp_table(self, clean_db):
        """Duplicate PK rows in temp table are deduped before promotion."""
        run_id = uuid.uuid4()
        tname = temp_table_name("dedup_test", run_id)
        create_table(clean_db, "temp_store", tname, COLS, unlogged=True)

        # Insert duplicate rows with same PK into temp table
        df = pd.DataFrame([
            {"id": 1, "name": "first", "score": 1.0},
            {"id": 1, "name": "second", "score": 2.0},  # duplicate PK
            {"id": 2, "name": "unique", "score": 3.0},
        ])
        write_to_temp(clean_db, tname, df)

        rows = promote(clean_db, tname, "raw", "dedup_target", COLS, PK, LoadStrategy.UPSERT)
        assert rows == 2  # only 2 unique PKs

        result = pd.read_sql("SELECT * FROM raw.dedup_target ORDER BY id", clean_db)
        assert len(result) == 2
        assert result[result["id"] == 1].iloc[0]["name"] in ("first", "second")
        assert result[result["id"] == 2].iloc[0]["name"] == "unique"


# ---------------------------------------------------------------------------
# Index creation
# ---------------------------------------------------------------------------


IDX_COLS = [Column("id", Integer(), nullable=False), Column("name", Text()), Column("status", Text())]
IDX_PK = ["id"]
INDEXES = [
    Index(columns=("name",)),
    Index(columns=("status",)),
]


@pytest.mark.integration
class TestEnsureIndexes:
    def test_creates_indexes(self, clean_db):
        create_table(clean_db, "raw", "idx_test", IDX_COLS, IDX_PK)
        ensure_indexes(clean_db, "raw", "idx_test", INDEXES)

        result = pd.read_sql(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'idx_test' AND schemaname = 'raw'",
            clean_db,
        )
        names = set(result["indexname"])
        assert "ix_idx_test_name" in names
        assert "ix_idx_test_status" in names

    def test_idempotent(self, clean_db):
        create_table(clean_db, "raw", "idx_idem", IDX_COLS, IDX_PK)
        ensure_indexes(clean_db, "raw", "idx_idem", INDEXES)
        ensure_indexes(clean_db, "raw", "idx_idem", INDEXES)  # no error

    def test_unique_index(self, clean_db):
        unique_idx = [Index(columns=("name",), unique=True)]
        create_table(clean_db, "raw", "idx_uniq", IDX_COLS, IDX_PK)
        ensure_indexes(clean_db, "raw", "idx_uniq", unique_idx)

        result = pd.read_sql(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'idx_uniq' AND schemaname = 'raw'",
            clean_db,
        )
        assert "ix_idx_uniq_name_unique" in set(result["indexname"])

    def test_promote_creates_indexes(self, clean_db):
        run_id = uuid.uuid4()
        tname = create_temp_table(clean_db, "idx_promo", run_id, IDX_COLS)
        df = pd.DataFrame({"id": [1, 2], "name": ["a", "b"], "status": ["open", "closed"]})
        write_to_temp(clean_db, tname, df)

        promote(clean_db, tname, "raw", "idx_promo", IDX_COLS, IDX_PK,
                LoadStrategy.FULL_REPLACE, indexes=INDEXES)

        result = pd.read_sql(
            "SELECT indexname FROM pg_indexes WHERE tablename = 'idx_promo' AND schemaname = 'raw'",
            clean_db,
        )
        names = set(result["indexname"])
        assert "ix_idx_promo_name" in names
        assert "ix_idx_promo_status" in names
