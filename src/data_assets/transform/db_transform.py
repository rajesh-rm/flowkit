"""Execute SQL transforms on Postgres source tables and load results into temp table."""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from data_assets.core.run_context import RunContext
from data_assets.load.loader import write_to_temp

logger = logging.getLogger(__name__)


def execute_transform(
    engine: Engine,
    query: str,
    temp_table: str,
    context: RunContext,
    timeout_seconds: int = 300,
) -> int:
    """Run a SQL query and write the results to a temp table.

    Args:
        engine: SQLAlchemy engine.
        query: SQL SELECT statement producing the output rows.
        temp_table: Name of the temp table in temp_store schema.
        context: Current run context.
        timeout_seconds: Per-query safety timeout (default 300s).

    Returns:
        Number of rows written.
    """
    logger.info("Executing transform query for '%s'", context.asset_name)
    with engine.begin() as conn:
        conn.execute(text(f"SET LOCAL statement_timeout = '{timeout_seconds}s'"))
        df = pd.read_sql(query, conn)
    rows = write_to_temp(engine, temp_table, df)
    logger.info(
        "Transform produced %d rows for '%s'", rows, context.asset_name
    )
    return rows
