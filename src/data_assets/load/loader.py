"""Unified loader: DDL, temp tables, and promotion in one module.

Handles the full data loading lifecycle:
- Schema DDL: create tables, add columns, drop tables
- Temp tables: create, write, read, check existence, drop
- Promotion: move data from temp → main table via full_replace/upsert/append

All dialect-specific SQL is delegated to ``db.dialect``.
"""

from __future__ import annotations

import logging
import uuid

import pandas as pd
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, SchemaContract
from data_assets.db.dialect import get_dialect

logger = logging.getLogger(__name__)

TEMP_SCHEMA = "temp_store"


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

def _column_ddl(col: Column, dialect=None) -> str:
    """Build the DDL fragment for a single column.

    If dialect is provided, compiles the type for that specific database.
    Otherwise defaults to Postgres (backward compatible).
    """
    if dialect is None:
        from sqlalchemy.dialects import postgresql
        dialect = postgresql.dialect()
    type_str = col.sa_type.compile(dialect=dialect)
    parts = [f'"{col.name}" {type_str}']
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

    from sqlalchemy import String, Text as SAText

    d = get_dialect(engine)
    pk_set = set(primary_key) if primary_key else set()

    # MariaDB cannot use TEXT columns in primary keys or unique indexes.
    # Auto-convert TEXT PK columns to VARCHAR(255) for MariaDB.
    adjusted_cols = []
    for c in columns:
        if c.name in pk_set and isinstance(c.sa_type, SAText) and engine.dialect.name in ("mysql", "mariadb"):
            adjusted_cols.append(Column(c.name, String(255), nullable=c.nullable, default=c.default))
        else:
            adjusted_cols.append(c)

    col_defs = ", ".join(d.column_ddl(c) for c in adjusted_cols)
    pk_clause = ""
    if primary_key:
        pk_cols = ", ".join(d.qi(c) for c in primary_key)
        pk_clause = f", PRIMARY KEY ({pk_cols})"

    create_kw = d.create_table_kw(unlogged)
    ddl = f"{create_kw} {d.fqn(schema, table_name)} ({col_defs}{pk_clause})"
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
    d = get_dialect(engine)
    with engine.begin() as conn:
        for col in new_cols:
            conn.execute(text(
                f"ALTER TABLE {d.fqn(schema, table_name)} ADD COLUMN {d.column_ddl(col)}"
            ))
            logger.info("Added column '%s' to %s.%s", col.name, schema, table_name)


def ensure_indexes(
    engine: Engine,
    schema: str,
    table_name: str,
    indexes: list[Index],
    columns: list[Column] | None = None,
) -> None:
    """Create declared indexes on the target table (idempotent).

    Uses CREATE INDEX IF NOT EXISTS so it is safe to call on every run.
    Each index is created in its own transaction so one failure does not
    block the others.
    """
    d = get_dialect(engine)
    column_types = {c.name: c.sa_type for c in columns} if columns else None
    for idx in indexes:
        ddl = d.create_index_ddl(schema, table_name, idx, column_types=column_types)
        with engine.begin() as conn:
            conn.execute(text(ddl))
        logger.debug("Ensured index on %s.%s", schema, table_name)


def drop_table(engine: Engine, schema: str, table_name: str) -> None:
    """Drop a table if it exists."""
    d = get_dialect(engine)
    with engine.begin() as conn:
        conn.execute(text(d.drop_table_ddl(schema, table_name)))


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
    """Create a temp table in temp_store. Returns the table name."""
    tname = temp_table_name(asset_name, run_id)
    create_table(engine, TEMP_SCHEMA, tname, columns, primary_key=None, unlogged=True)
    logger.debug("Created temp table %s.%s", TEMP_SCHEMA, tname)
    return tname


def write_to_temp(engine: Engine, table_name: str, df: pd.DataFrame) -> int:
    """Append a DataFrame to the temp table. Returns rows written.

    Datetime columns containing ISO 8601 strings (e.g., '2025-12-01T08:00:00Z')
    are converted to proper datetime objects before writing. This ensures
    MariaDB DATETIME columns accept the values (MariaDB rejects the 'Z' suffix
    and 'T' separator that Postgres auto-parses).
    """
    if df.empty:
        return 0
    rows = len(df)

    # Convert datetime-like string columns to proper datetime objects.
    # MariaDB DATETIME rejects ISO 8601 strings with 'T' separator or
    # timezone suffixes ('Z', '+00:00'). Convert to naive UTC datetimes
    # so pandas sends format-agnostic values to any backend.
    is_mariadb = engine.dialect.name in ("mysql", "mariadb")
    df = df.copy()
    for col in df.columns:
        if df[col].dtype in ("object", "str", "string") and len(df) > 0:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, str) and ("T" in sample or "Z" in sample):
                try:
                    converted = pd.to_datetime(df[col], utc=True, errors="coerce")
                    # Strip timezone for MariaDB (DATETIME is tz-naive)
                    if is_mariadb:
                        converted = converted.dt.tz_localize(None)
                    df[col] = converted
                except Exception:
                    pass  # not a datetime column
        # Also strip tz from already-parsed datetime columns for MariaDB
        elif is_mariadb and hasattr(df[col].dtype, "tz") and df[col].dtype.tz is not None:
            df[col] = df[col].dt.tz_localize(None)

    df.to_sql(
        table_name, engine, schema=TEMP_SCHEMA,
        if_exists="append", index=False, method="multi",
        chunksize=1000,
    )
    logger.debug("Wrote %d rows to %s.%s", rows, TEMP_SCHEMA, table_name)
    return rows


def read_temp_table(engine: Engine, table_name: str) -> pd.DataFrame:
    """Read the entire temp table into a DataFrame."""
    d = get_dialect(engine)
    return pd.read_sql(f"SELECT * FROM {d.fqn(TEMP_SCHEMA, table_name)}", engine)


def drop_temp_table(engine: Engine, table_name: str) -> None:
    """Drop a temp table after successful promotion."""
    drop_table(engine, TEMP_SCHEMA, table_name)


def temp_table_exists(engine: Engine, table_name: str) -> bool:
    """Check if a temp table exists."""
    return inspect(engine).has_table(table_name, schema=TEMP_SCHEMA)


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
    d = get_dialect(engine)
    promoter = _PROMOTERS[load_strategy.value]

    with engine.begin() as conn:
        # Dedup: remove duplicate PK rows within the temp table before promotion.
        # Resumed or inherited temp tables can contain duplicates from retries.
        if primary_key:
            removed = d.dedup_temp_table(conn, TEMP_SCHEMA, temp_table, primary_key)
            if removed > 0:
                logger.warning(
                    "Removed %d duplicate rows from temp table before promotion",
                    removed,
                )

        rows_loaded = promoter(conn, d, TEMP_SCHEMA, temp_table, target_schema,
                               target_table, primary_key, column_names)

    if indexes:
        ensure_indexes(engine, target_schema, target_table, indexes, columns)

    logger.info("Promoted %d rows to %s.%s via %s",
                rows_loaded, target_schema, target_table, load_strategy.value)
    return rows_loaded


def _promote_full_replace(conn, d, temp_schema, temp_table, main_schema, main_table,
                          primary_key, column_names) -> int:
    """Truncate main table, then INSERT...SELECT from temp."""
    conn.execute(text(f"TRUNCATE TABLE {d.fqn(main_schema, main_table)}"))
    cols = ", ".join(d.qi(c) for c in column_names)
    result = conn.execute(text(
        f"INSERT INTO {d.fqn(main_schema, main_table)} ({cols}) "
        f"SELECT {cols} FROM {d.fqn(temp_schema, temp_table)}"
    ))
    return result.rowcount


def _promote_upsert(conn, d, temp_schema, temp_table, main_schema, main_table,
                    primary_key, column_names) -> int:
    """INSERT with conflict handling from temp, using dialect-specific SQL."""
    sql = d.upsert_sql(main_schema, main_table, temp_schema, temp_table,
                       primary_key, column_names)
    return conn.execute(text(sql)).rowcount


def _promote_append(conn, d, temp_schema, temp_table, main_schema, main_table,
                    primary_key, column_names) -> int:
    """INSERT...SELECT from temp (no conflict handling)."""
    cols = ", ".join(d.qi(c) for c in column_names)
    return conn.execute(text(
        f"INSERT INTO {d.fqn(main_schema, main_table)} ({cols}) "
        f"SELECT {cols} FROM {d.fqn(temp_schema, temp_table)}"
    )).rowcount


_PROMOTERS = {
    "full_replace": _promote_full_replace,
    "upsert": _promote_upsert,
    "append": _promote_append,
}
