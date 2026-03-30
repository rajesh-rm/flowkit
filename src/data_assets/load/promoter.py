"""Temp table → main table promotion orchestrator."""

from __future__ import annotations

import logging

from sqlalchemy.engine import Engine

from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy
from data_assets.load.schema_manager import create_table, ensure_columns
from data_assets.load.strategies import STRATEGY_MAP

logger = logging.getLogger(__name__)

TEMP_SCHEMA = "temp_store"


def promote(
    engine: Engine,
    temp_table: str,
    target_schema: str,
    target_table: str,
    columns: list[Column],
    primary_key: list[str],
    load_strategy: LoadStrategy,
) -> int:
    """Promote data from temp table to main table.

    Ensures the target table exists (creates if missing, adds new columns).
    Runs the promotion strategy in a single transaction.
    Returns number of rows loaded.
    """
    # Ensure main table exists with correct schema
    create_table(
        engine, target_schema, target_table, columns, primary_key
    )
    ensure_columns(engine, target_schema, target_table, columns)

    strategy_cls = STRATEGY_MAP[load_strategy.value]
    strategy = strategy_cls()
    column_names = [c.name for c in columns]

    with engine.begin() as conn:
        rows_loaded = strategy.promote(
            conn=conn,
            temp_schema=TEMP_SCHEMA,
            temp_table=temp_table,
            target_schema=target_schema,
            target_table=target_table,
            primary_key=primary_key,
            column_names=column_names,
        )

    logger.info(
        "Promoted %d rows from %s.%s to %s.%s via %s",
        rows_loaded,
        TEMP_SCHEMA,
        temp_table,
        target_schema,
        target_table,
        load_strategy.value,
    )
    return rows_loaded
