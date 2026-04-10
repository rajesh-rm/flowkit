"""Database dialect abstraction layer.

Centralises all dialect-specific SQL operations so the rest of the
codebase never writes raw Postgres or MariaDB SQL directly.

Supported backends:
- PostgreSQL 16+ (dialect name: "postgresql")
- MariaDB 10.11+ (dialect name: "mysql")

Usage:
    from data_assets.db.dialect import get_dialect
    d = get_dialect(engine)
    d.set_query_timeout(conn, 300)
    d.dedup_temp_table(conn, schema, table, pk_cols)
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from data_assets.core.column import Column, Index, index_name
from data_assets.core.enums import IndexMethod

logger = logging.getLogger(__name__)

# Index methods supported by MariaDB (others fall back to BTREE)
_MARIADB_INDEX_METHODS = {IndexMethod.BTREE, IndexMethod.HASH}


class Dialect(ABC):
    """Abstract interface for dialect-specific SQL operations."""

    @abstractmethod
    def set_query_timeout(self, conn: Connection, seconds: int) -> None:
        """Set a per-query timeout for the current transaction/session."""

    @abstractmethod
    def column_ddl(self, col: Column) -> str:
        """Build the DDL fragment for a single column."""

    @abstractmethod
    def create_table_kw(self, unlogged: bool) -> str:
        """Return the CREATE keyword (e.g., 'CREATE UNLOGGED TABLE')."""

    @abstractmethod
    def dedup_temp_table(
        self, conn: Connection, schema: str, table: str, pk_cols: list[str],
    ) -> int:
        """Remove duplicate PK rows from a temp table. Returns rows removed."""

    @abstractmethod
    def upsert_sql(
        self, main_schema: str, main_table: str,
        temp_schema: str, temp_table: str,
        pk_cols: list[str], column_names: list[str],
    ) -> str:
        """Build the UPSERT SQL (INSERT ... ON CONFLICT/DUPLICATE KEY)."""

    @abstractmethod
    def create_index_ddl(
        self, schema: str, table_name: str, idx: Index,
    ) -> str:
        """Build CREATE INDEX DDL for this dialect."""

    @abstractmethod
    def drop_table_ddl(self, schema: str, table_name: str) -> str:
        """Build DROP TABLE DDL for this dialect."""


class PostgresDialect(Dialect):
    """PostgreSQL 16+ dialect."""

    def set_query_timeout(self, conn: Connection, seconds: int) -> None:
        conn.execute(text(f"SET LOCAL statement_timeout = '{seconds}s'"))

    def column_ddl(self, col: Column) -> str:
        from sqlalchemy.dialects import postgresql
        type_str = col.sa_type.compile(dialect=postgresql.dialect())
        parts = [f'"{col.name}" {type_str}']
        if not col.nullable:
            parts.append("NOT NULL")
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")
        return " ".join(parts)

    def create_table_kw(self, unlogged: bool) -> str:
        return "CREATE UNLOGGED TABLE" if unlogged else "CREATE TABLE"

    def dedup_temp_table(
        self, conn: Connection, schema: str, table: str, pk_cols: list[str],
    ) -> int:
        pk = ", ".join(f'"{c}"' for c in pk_cols)
        result = conn.execute(text(
            f'DELETE FROM "{schema}"."{table}" a '
            f"USING (SELECT ctid, ROW_NUMBER() OVER "
            f"(PARTITION BY {pk} ORDER BY ctid DESC) AS rn "
            f'FROM "{schema}"."{table}") b '
            f"WHERE a.ctid = b.ctid AND b.rn > 1"
        ))
        return result.rowcount

    def upsert_sql(
        self, main_schema: str, main_table: str,
        temp_schema: str, temp_table: str,
        pk_cols: list[str], column_names: list[str],
    ) -> str:
        cols = ", ".join(f'"{c}"' for c in column_names)
        pk = ", ".join(f'"{c}"' for c in pk_cols)
        non_pk = [c for c in column_names if c not in pk_cols]
        if non_pk:
            update = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in non_pk)
            return (
                f'INSERT INTO "{main_schema}"."{main_table}" ({cols}) '
                f'SELECT {cols} FROM "{temp_schema}"."{temp_table}" '
                f"ON CONFLICT ({pk}) DO UPDATE SET {update}"
            )
        return (
            f'INSERT INTO "{main_schema}"."{main_table}" ({cols}) '
            f'SELECT {cols} FROM "{temp_schema}"."{temp_table}" '
            f"ON CONFLICT ({pk}) DO NOTHING"
        )

    def create_index_ddl(
        self, schema: str, table_name: str, idx: Index,
    ) -> str:
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
        return ddl

    def drop_table_ddl(self, schema: str, table_name: str) -> str:
        return f'DROP TABLE IF EXISTS "{schema}"."{table_name}" CASCADE'


class MariaDBDialect(Dialect):
    """MariaDB 10.11+ dialect."""

    def set_query_timeout(self, conn: Connection, seconds: int) -> None:
        conn.execute(text(f"SET max_statement_time = {seconds}"))

    def column_ddl(self, col: Column) -> str:
        from sqlalchemy.dialects import mysql
        type_str = col.sa_type.compile(dialect=mysql.dialect())
        parts = [f'`{col.name}` {type_str}']
        if not col.nullable:
            parts.append("NOT NULL")
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")
        return " ".join(parts)

    def create_table_kw(self, unlogged: bool) -> str:
        # MariaDB has no UNLOGGED tables — use regular CREATE TABLE
        if unlogged:
            logger.debug("MariaDB: UNLOGGED not supported, using regular table")
        return "CREATE TABLE"

    def dedup_temp_table(
        self, conn: Connection, schema: str, table: str, pk_cols: list[str],
    ) -> int:
        # MariaDB has no ctid (physical row ID). The safe approach is a
        # three-step swap: copy distinct rows to a temp table, truncate
        # the original, then insert back.
        cols_sql = ", ".join(f'`{c}`' for c in pk_cols)
        dedup_tbl = f"_dedup_{table}"

        # Count before dedup
        before = conn.execute(text(
            f"SELECT COUNT(*) FROM `{schema}`.`{table}`"
        )).scalar()

        # Step 1: Copy distinct rows (keep one per PK group)
        conn.execute(text(
            f"CREATE TEMPORARY TABLE `{dedup_tbl}` AS "
            f"SELECT * FROM `{schema}`.`{table}` GROUP BY {cols_sql}"
        ))

        # Step 2: Truncate original
        conn.execute(text(f"TRUNCATE TABLE `{schema}`.`{table}`"))

        # Step 3: Insert deduped rows back
        conn.execute(text(
            f"INSERT INTO `{schema}`.`{table}` SELECT * FROM `{dedup_tbl}`"
        ))

        # Step 4: Cleanup
        conn.execute(text(f"DROP TEMPORARY TABLE `{dedup_tbl}`"))

        after = conn.execute(text(
            f"SELECT COUNT(*) FROM `{schema}`.`{table}`"
        )).scalar()
        return before - after

    def upsert_sql(
        self, main_schema: str, main_table: str,
        temp_schema: str, temp_table: str,
        pk_cols: list[str], column_names: list[str],
    ) -> str:
        cols = ", ".join(f'`{c}`' for c in column_names)
        non_pk = [c for c in column_names if c not in pk_cols]
        if non_pk:
            update = ", ".join(f'`{c}` = VALUES(`{c}`)' for c in non_pk)
            return (
                f"INSERT INTO `{main_schema}`.`{main_table}` ({cols}) "
                f"SELECT {cols} FROM `{temp_schema}`.`{temp_table}` "
                f"ON DUPLICATE KEY UPDATE {update}"
            )
        # No non-PK columns — INSERT IGNORE to skip duplicates
        return (
            f"INSERT IGNORE INTO `{main_schema}`.`{main_table}` ({cols}) "
            f"SELECT {cols} FROM `{temp_schema}`.`{temp_table}`"
        )

    def create_index_ddl(
        self, schema: str, table_name: str, idx: Index,
    ) -> str:
        name = index_name(table_name, idx)
        unique = "UNIQUE " if idx.unique else ""
        cols = ", ".join(f'`{c}`' for c in idx.columns)

        method = idx.method
        if method not in _MARIADB_INDEX_METHODS:
            logger.warning(
                "MariaDB does not support index method '%s' — falling back to BTREE "
                "for index '%s' on %s.%s",
                method.value, name, schema, table_name,
            )
            method = IndexMethod.BTREE

        ddl = (
            f'CREATE {unique}INDEX IF NOT EXISTS `{name}` '
            f'ON `{schema}`.`{table_name}` USING {method} ({cols})'
        )
        # MariaDB does not support INCLUDE clause
        if idx.include:
            logger.info(
                "MariaDB: skipping INCLUDE clause for index '%s' on %s.%s "
                "(not supported)",
                name, schema, table_name,
            )
        if idx.where:
            # MariaDB does not support partial indexes (WHERE clause)
            logger.info(
                "MariaDB: skipping WHERE clause for index '%s' on %s.%s "
                "(partial indexes not supported)",
                name, schema, table_name,
            )
        return ddl

    def drop_table_ddl(self, schema: str, table_name: str) -> str:
        # MariaDB does not support CASCADE on DROP TABLE
        return f'DROP TABLE IF EXISTS `{schema}`.`{table_name}`'


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_DIALECTS: dict[str, Dialect] = {}


def get_dialect(engine: Engine) -> Dialect:
    """Return the appropriate Dialect instance for the given engine.

    Caches by dialect name so only one instance is created per backend.
    """
    name = engine.dialect.name
    if name not in _DIALECTS:
        if name == "postgresql":
            _DIALECTS[name] = PostgresDialect()
        elif name in ("mysql", "mariadb"):
            _DIALECTS[name] = MariaDBDialect()
        else:
            raise ValueError(
                f"Unsupported database dialect '{name}'. "
                f"data-assets supports PostgreSQL 16+ and MariaDB 10.11+."
            )
    return _DIALECTS[name]
