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

from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy

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
    schema_contract: str = "evolve",
) -> None:
    """Manage column differences between asset definition and table.

    Schema contracts:
        "evolve"  — auto-add new columns (default)
        "freeze"  — raise error if definition has columns not in table
        "discard" — silently ignore new columns
    """
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
    logger.info("Created temp table %s.%s", TEMP_SCHEMA, tname)
    return tname


def write_to_temp(engine: Engine, table_name: str, df: pd.DataFrame) -> int:
    """Append a DataFrame to the temp table. Returns rows written."""
    if df.empty:
        return 0
    rows = len(df)
    df.to_sql(table_name, engine, schema=TEMP_SCHEMA, if_exists="append", index=False, method="multi")
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
) -> int:
    """Promote data from temp table to main table in a single transaction.

    Ensures target table exists (creates if missing, manages columns per schema_contract).
    Returns number of rows loaded.
    """
    create_table(engine, target_schema, target_table, columns, primary_key)
    ensure_columns(engine, target_schema, target_table, columns, schema_contract)

    column_names = [c.name for c in columns]
    promoter = _PROMOTERS[load_strategy.value]

    with engine.begin() as conn:
        rows_loaded = promoter(conn, TEMP_SCHEMA, temp_table, target_schema, target_table,
                               primary_key, column_names)

    logger.info("Promoted %d rows to %s.%s via %s",
                rows_loaded, target_schema, target_table, load_strategy.value)
    return rows_loaded


def _promote_full_replace(conn, ts, tt, ms, mt, pk, cols) -> int:
    """Truncate main table, then INSERT...SELECT from temp."""
    conn.execute(text(f'TRUNCATE TABLE "{ms}"."{mt}"'))
    c = ", ".join(f'"{c}"' for c in cols)
    result = conn.execute(text(
        f'INSERT INTO "{ms}"."{mt}" ({c}) SELECT {c} FROM "{ts}"."{tt}"'
    ))
    return result.rowcount


def _promote_upsert(conn, ts, tt, ms, mt, pk, cols) -> int:
    """INSERT...ON CONFLICT DO UPDATE from temp."""
    c = ", ".join(f'"{x}"' for x in cols)
    pk_c = ", ".join(f'"{x}"' for x in pk)
    non_pk = [x for x in cols if x not in pk]
    if non_pk:
        update = ", ".join(f'"{x}" = EXCLUDED."{x}"' for x in non_pk)
        sql = (f'INSERT INTO "{ms}"."{mt}" ({c}) SELECT {c} FROM "{ts}"."{tt}" '
               f"ON CONFLICT ({pk_c}) DO UPDATE SET {update}")
    else:
        sql = (f'INSERT INTO "{ms}"."{mt}" ({c}) SELECT {c} FROM "{ts}"."{tt}" '
               f"ON CONFLICT ({pk_c}) DO NOTHING")
    return conn.execute(text(sql)).rowcount


def _promote_append(conn, ts, tt, ms, mt, pk, cols) -> int:
    """INSERT...SELECT from temp (no conflict handling)."""
    c = ", ".join(f'"{x}"' for x in cols)
    return conn.execute(text(
        f'INSERT INTO "{ms}"."{mt}" ({c}) SELECT {c} FROM "{ts}"."{tt}"'
    )).rowcount


_PROMOTERS = {
    "full_replace": _promote_full_replace,
    "upsert": _promote_upsert,
    "append": _promote_append,
}
