"""Execute SQL transforms on Postgres source tables and load results into temp table."""

from __future__ import annotations

import logging

import pandas as pd
from sqlalchemy.engine import Engine

from data_assets.core.run_context import RunContext
from data_assets.load.temp_table import write_to_temp

logger = logging.getLogger(__name__)


def execute_transform(
    engine: Engine,
    query: str,
    temp_table: str,
    context: RunContext,
) -> int:
    """Run a SQL query and write the results to a temp table.

    Args:
        engine: SQLAlchemy engine.
        query: SQL SELECT statement producing the output rows.
        temp_table: Name of the temp table in temp_store schema.
        context: Current run context.

    Returns:
        Number of rows written.
    """
    logger.info("Executing transform query for '%s'", context.asset_name)
    df = pd.read_sql(query, engine)
    rows = write_to_temp(engine, temp_table, df)
    logger.info(
        "Transform produced %d rows for '%s'", rows, context.asset_name
    )
    return rows
