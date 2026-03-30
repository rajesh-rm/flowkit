"""DDL management: create tables, compare schemas, add columns."""

from __future__ import annotations

import logging

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from data_assets.core.column import Column

logger = logging.getLogger(__name__)


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
    """CREATE TABLE from a list of Column definitions (idempotent)."""
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
) -> None:
    """Add any new columns defined in the asset but missing from the table.

    Only additive — never drops or alters existing columns.
    """
    insp = inspect(engine)
    if not insp.has_table(table_name, schema=schema):
        return

    existing = {c["name"] for c in insp.get_columns(table_name, schema=schema)}
    new_cols = [c for c in columns if c.name not in existing]

    if not new_cols:
        return

    with engine.begin() as conn:
        for col in new_cols:
            ddl = (
                f'ALTER TABLE "{schema}"."{table_name}" '
                f"ADD COLUMN {_column_ddl(col)}"
            )
            conn.execute(text(ddl))
            logger.info(
                "Added column '%s' to %s.%s", col.name, schema, table_name
            )


def check_schema_compatibility(
    engine: Engine,
    schema: str,
    table_name: str,
    columns: list[Column],
) -> list[str]:
    """Compare asset column defs against the existing table.

    Returns a list of warnings/errors (empty = compatible).
    """
    insp = inspect(engine)
    if not insp.has_table(table_name, schema=schema):
        return []

    existing_cols = {c["name"]: c for c in insp.get_columns(table_name, schema=schema)}
    defined_names = {c.name for c in columns}
    issues: list[str] = []

    # Columns removed from definition but still in table
    for name in existing_cols:
        if name not in defined_names:
            issues.append(
                f"Column '{name}' exists in table but not in asset definition "
                "(will NOT be dropped)"
            )

    return issues


def drop_table(engine: Engine, schema: str, table_name: str) -> None:
    """Drop a table if it exists."""
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{table_name}"'))
    logger.debug("Dropped table %s.%s", schema, table_name)
