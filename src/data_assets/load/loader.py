"""Unified loader: DDL, temp tables, and promotion in one module.

Handles the full data loading lifecycle:
- Schema DDL: create tables, add columns, drop tables
- Temp tables: create (UNLOGGED), write, read, check existence, drop
- Promotion: move data from temp → main table via full_replace/upsert/append
"""

from __future__ import annotations

import logging
import uuid

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from data_assets.core.column import Column, Index, index_name
from data_assets.core.enums import LoadStrategy, SchemaContract

logger = logging.getLogger(__name__)

TEMP_SCHEMA = "temp_store"


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

def _column_ddl(col: Column) -> str:
    """Build the DDL fragment for a single column."""
    parts = [f'"{col.name}" {col.pg_type}']
    if not col.nullable:
        parts.append("NOT NULL")
    if col.default is not None:
        parts.append(f"DEFAULT {col.default}")
    return " ".join(parts)


def create_table(
    engine: Engine,
    schema: str,
    table_name: str,
    columns: list[Column],
    primary_key: list[str] | None = None,
    unlogged: bool = False,
) -> None:
    """CREATE TABLE from Column definitions (idempotent)."""
    insp = inspect(engine)
    if insp.has_table(table_name, schema=schema):
        return

    col_defs = ", ".join(_column_ddl(c) for c in columns)
    pk_clause = ""
    if primary_key:
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        pk_clause = f", PRIMARY KEY ({pk_cols})"

    unlogged_kw = "UNLOGGED " if unlogged else ""
    ddl = (
        f'CREATE {unlogged_kw}TABLE "{schema}"."{table_name}" '
        f"({col_defs}{pk_clause})"
    )
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Created table %s.%s", schema, table_name)


def ensure_columns(
    engine: Engine,
    schema: str,
    table_name: str,
    columns: list[Column],
    schema_contract: str | SchemaContract = "evolve",
) -> None:
    """Manage column differences between asset definition and table."""
    insp = inspect(engine)
    if not insp.has_table(table_name, schema=schema):
        return

    existing = {c["name"] for c in insp.get_columns(table_name, schema=schema)}
    new_cols = [c for c in columns if c.name not in existing]
    if not new_cols:
        return

    if schema_contract == "freeze":
        names = [c.name for c in new_cols]
        raise ValueError(
            f"Schema contract 'freeze' violated: new columns {names} "
            f"not in {schema}.{table_name}. Manually add them or change to 'evolve'."
        )

    if schema_contract == "discard":
        logger.info(
            "Schema contract 'discard': ignoring %d new columns for %s.%s",
            len(new_cols), schema, table_name,
        )
        return

    # Default: evolve — auto-add
    with engine.begin() as conn:
        for col in new_cols:
            conn.execute(text(
                f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN {_column_ddl(col)}'
            ))
            logger.info("Added column '%s' to %s.%s", col.name, schema, table_name)


def ensure_indexes(
    engine: Engine,
    schema: str,
    table_name: str,
    indexes: list[Index],
) -> None:
    """Create declared indexes on the target table (idempotent).

    Uses CREATE INDEX IF NOT EXISTS so it is safe to call on every run.
    Each index is created in its own transaction so one failure does not
    block the others.
    """
    for idx in indexes:
        name = index_name(table_name, idx)
        unique = "UNIQUE " if idx.unique else ""
        cols = ", ".join(f'"{c}"' for c in idx.columns)
        ddl = (
            f'CREATE {unique}INDEX IF NOT EXISTS "{name}" '
            f'ON "{schema}"."{table_name}" USING {idx.method} ({cols})'
        )
        if idx.include:
            inc_cols = ", ".join(f'"{c}"' for c in idx.include)
            ddl += f" INCLUDE ({inc_cols})"
        if idx.where:
            ddl += f" WHERE {idx.where}"
        with engine.begin() as conn:
            conn.execute(text(ddl))
        logger.debug("Ensured index %s on %s.%s", name, schema, table_name)


def drop_table(engine: Engine, schema: str, table_name: str) -> None:
    """Drop a table if it exists."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"'))


# ---------------------------------------------------------------------------
# Temp table operations
# ---------------------------------------------------------------------------

def temp_table_name(asset_name: str, run_id: uuid.UUID) -> str:
    """Generate a deterministic temp table name for a run."""
    short_id = str(run_id).replace("-", "")[:12]
    return f"{asset_name}_{short_id}"


def create_temp_table(
    engine: Engine,
    asset_name: str,
    run_id: uuid.UUID,
    columns: list[Column],
) -> str:
    """Create an UNLOGGED temp table in temp_store. Returns the table name."""
    tname = temp_table_name(asset_name, run_id)
    create_table(engine, TEMP_SCHEMA, tname, columns, primary_key=None, unlogged=True)
    logger.debug("Created temp table %s.%s", TEMP_SCHEMA, tname)
    return tname


def write_to_temp(engine: Engine, table_name: str, df: pd.DataFrame) -> int:
    """Append a DataFrame to the temp table. Returns rows written."""
    if df.empty:
        return 0
    rows = len(df)
    df.to_sql(
        table_name, engine, schema=TEMP_SCHEMA,
        if_exists="append", index=False, method="multi",
        chunksize=1000,
    )
    logger.debug("Wrote %d rows to %s.%s", rows, TEMP_SCHEMA, table_name)
    return rows


def read_temp_table(engine: Engine, table_name: str) -> pd.DataFrame:
    """Read the entire temp table into a DataFrame."""
    return pd.read_sql(f'SELECT * FROM "{TEMP_SCHEMA}"."{table_name}"', engine)


def drop_temp_table(engine: Engine, table_name: str) -> None:
    """Drop a temp table after successful promotion."""
    drop_table(engine, TEMP_SCHEMA, table_name)


def temp_table_exists(engine: Engine, table_name: str) -> bool:
    """Check if a temp table exists."""
    with engine.connect() as conn:
        result = conn.execute(text(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name = :table)"
        ), {"schema": TEMP_SCHEMA, "table": table_name})
        return result.scalar()


# ---------------------------------------------------------------------------
# Promotion: temp → main table
# ---------------------------------------------------------------------------

def promote(
    engine: Engine,
    temp_table: str,
    target_schema: str,
    target_table: str,
    columns: list[Column],
    primary_key: list[str],
    load_strategy: LoadStrategy,
    schema_contract: str = "evolve",
    indexes: list[Index] | None = None,
) -> int:
    """Promote data from temp table to main table in a single transaction.

    Ensures target table exists (creates if missing, manages columns per schema_contract).
    Creates declared indexes after promotion (idempotent).
    Returns number of rows loaded.
    """
    create_table(engine, target_schema, target_table, columns, primary_key)
    ensure_columns(engine, target_schema, target_table, columns, schema_contract)

    column_names = [c.name for c in columns]
    promoter = _PROMOTERS[load_strategy.value]

    with engine.begin() as conn:
        # WHY: Resumed or inherited temp tables can contain duplicate PK rows
        # from retries or partial prior runs. ON CONFLICT in the promoter
        # handles main↔temp conflicts, but not duplicates WITHIN the temp
        # table itself. We must dedup here to guarantee idempotent promotion.
        # Uses ctid (Postgres physical row ID) to keep the last-inserted copy.
        if primary_key:
            pk_cols = ", ".join(f'"{c}"' for c in primary_key)
            result = conn.execute(text(
                f'DELETE FROM "{TEMP_SCHEMA}"."{temp_table}" a '
                f"USING (SELECT ctid, ROW_NUMBER() OVER "
                f"(PARTITION BY {pk_cols} ORDER BY ctid DESC) AS rn "
                f'FROM "{TEMP_SCHEMA}"."{temp_table}") b '
                f"WHERE a.ctid = b.ctid AND b.rn > 1"
            ))
            if result.rowcount > 0:
                logger.warning(
                    "Removed %d duplicate rows from temp table before promotion",
                    result.rowcount,
                )

        rows_loaded = promoter(conn, TEMP_SCHEMA, temp_table, target_schema, target_table,
                               primary_key, column_names)

    if indexes:
        ensure_indexes(engine, target_schema, target_table, indexes)

    logger.info("Promoted %d rows to %s.%s via %s",
                rows_loaded, target_schema, target_table, load_strategy.value)
    return rows_loaded


def _promote_full_replace(conn, temp_schema, temp_table, main_schema, main_table,
                          primary_key, column_names) -> int:
    """Truncate main table, then INSERT...SELECT from temp."""
    conn.execute(text(f'TRUNCATE TABLE "{main_schema}"."{main_table}"'))
    cols = ", ".join(f'"{c}"' for c in column_names)
    result = conn.execute(text(
        f'INSERT INTO "{main_schema}"."{main_table}" ({cols}) '
        f'SELECT {cols} FROM "{temp_schema}"."{temp_table}"'
    ))
    return result.rowcount


def _promote_upsert(conn, temp_schema, temp_table, main_schema, main_table,
                    primary_key, column_names) -> int:
    """INSERT...ON CONFLICT DO UPDATE from temp."""
    cols = ", ".join(f'"{c}"' for c in column_names)
    pk_cols = ", ".join(f'"{c}"' for c in primary_key)
    non_pk = [c for c in column_names if c not in primary_key]
    if non_pk:
        update = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk)
        sql = (f'INSERT INTO "{main_schema}"."{main_table}" ({cols}) '
               f'SELECT {cols} FROM "{temp_schema}"."{temp_table}" '
               f"ON CONFLICT ({pk_cols}) DO UPDATE SET {update}")
    else:
        sql = (f'INSERT INTO "{main_schema}"."{main_table}" ({cols}) '
               f'SELECT {cols} FROM "{temp_schema}"."{temp_table}" '
               f"ON CONFLICT ({pk_cols}) DO NOTHING")
    return conn.execute(text(sql)).rowcount


def _promote_append(conn, temp_schema, temp_table, main_schema, main_table,
                    primary_key, column_names) -> int:
    """INSERT...SELECT from temp (no conflict handling)."""
    cols = ", ".join(f'"{c}"' for c in column_names)
    return conn.execute(text(
        f'INSERT INTO "{main_schema}"."{main_table}" ({cols}) '
        f'SELECT {cols} FROM "{temp_schema}"."{temp_table}"'
    )).rowcount


_PROMOTERS = {
    "full_replace": _promote_full_replace,
    "upsert": _promote_upsert,
    "append": _promote_append,
}
