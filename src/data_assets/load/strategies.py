"""Promotion strategies: full replace, upsert, append."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


class PromotionStrategy(ABC):
    """Base class for temp → main table promotion."""

    @abstractmethod
    def promote(
        self,
        conn: Connection,
        temp_schema: str,
        temp_table: str,
        target_schema: str,
        target_table: str,
        primary_key: list[str],
        column_names: list[str],
    ) -> int:
        """Promote rows from temp to main table within a transaction.

        Returns number of rows loaded into the main table.
        """
        ...


class FullReplaceStrategy(PromotionStrategy):
    """Truncate the main table and INSERT…SELECT from temp."""

    def promote(self, conn, temp_schema, temp_table, target_schema, target_table,
                primary_key, column_names) -> int:
        conn.execute(text(f'TRUNCATE TABLE "{target_schema}"."{target_table}"'))
        cols = ", ".join(f'"{c}"' for c in column_names)
        result = conn.execute(text(
            f'INSERT INTO "{target_schema}"."{target_table}" ({cols}) '
            f'SELECT {cols} FROM "{temp_schema}"."{temp_table}"'
        ))
        count = result.rowcount
        logger.info("Full replace: loaded %d rows into %s.%s", count, target_schema, target_table)
        return count


class UpsertStrategy(PromotionStrategy):
    """INSERT…ON CONFLICT DO UPDATE from temp."""

    def promote(self, conn, temp_schema, temp_table, target_schema, target_table,
                primary_key, column_names) -> int:
        cols = ", ".join(f'"{c}"' for c in column_names)
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        non_pk = [c for c in column_names if c not in primary_key]
        update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk)

        if non_pk:
            sql = (
                f'INSERT INTO "{target_schema}"."{target_table}" ({cols}) '
                f'SELECT {cols} FROM "{temp_schema}"."{temp_table}" '
                f"ON CONFLICT ({pk_cols}) DO UPDATE SET {update_clause}"
            )
        else:
            sql = (
                f'INSERT INTO "{target_schema}"."{target_table}" ({cols}) '
                f'SELECT {cols} FROM "{temp_schema}"."{temp_table}" '
                f"ON CONFLICT ({pk_cols}) DO NOTHING"
            )

        result = conn.execute(text(sql))
        count = result.rowcount
        logger.info("Upsert: loaded %d rows into %s.%s", count, target_schema, target_table)
        return count


class AppendStrategy(PromotionStrategy):
    """INSERT…SELECT from temp (no conflict handling)."""

    def promote(self, conn, temp_schema, temp_table, target_schema, target_table,
                primary_key, column_names) -> int:
        cols = ", ".join(f'"{c}"' for c in column_names)
        result = conn.execute(text(
            f'INSERT INTO "{target_schema}"."{target_table}" ({cols}) '
            f'SELECT {cols} FROM "{temp_schema}"."{temp_table}"'
        ))
        count = result.rowcount
        logger.info("Append: loaded %d rows into %s.%s", count, target_schema, target_table)
        return count


STRATEGY_MAP = {
    "full_replace": FullReplaceStrategy,
    "upsert": UpsertStrategy,
    "append": AppendStrategy,
}
