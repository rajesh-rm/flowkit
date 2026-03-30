"""Temp table lifecycle: create, write, read, and drop in temp_store schema."""

from __future__ import annotations

import logging
import uuid

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from data_assets.core.column import Column
from data_assets.load.schema_manager import create_table, drop_table

logger = logging.getLogger(__name__)

TEMP_SCHEMA = "temp_store"


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
    create_table(
        engine,
        schema=TEMP_SCHEMA,
        table_name=tname,
        columns=columns,
        primary_key=None,  # No PK constraint on temp tables
        unlogged=True,
    )
    logger.info("Created temp table %s.%s", TEMP_SCHEMA, tname)
    return tname


def write_to_temp(
    engine: Engine, table_name: str, df: pd.DataFrame
) -> int:
    """Append a DataFrame to the temp table. Returns number of rows written."""
    if df.empty:
        return 0

    qualified = f"{TEMP_SCHEMA}.{table_name}"
    rows = len(df)
    df.to_sql(
        table_name,
        engine,
        schema=TEMP_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.debug("Wrote %d rows to %s", rows, qualified)
    return rows


def read_temp_table(engine: Engine, table_name: str) -> pd.DataFrame:
    """Read the entire temp table into a DataFrame."""
    query = f'SELECT * FROM "{TEMP_SCHEMA}"."{table_name}"'
    return pd.read_sql(query, engine)


def drop_temp_table(engine: Engine, table_name: str) -> None:
    """Drop a temp table after successful promotion."""
    drop_table(engine, TEMP_SCHEMA, table_name)


def temp_table_exists(engine: Engine, table_name: str) -> bool:
    """Check if a temp table exists."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT EXISTS ("
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :schema AND table_name = :table"
                ")"
            ),
            {"schema": TEMP_SCHEMA, "table": table_name},
        )
        return result.scalar()
