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

import functools
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
    def qi(self, name: str) -> str:
        """Quote a SQL identifier (table, column, schema name)."""

    def fqn(self, schema: str, table: str) -> str:
        """Return a fully qualified table name: schema.table."""
        return f"{self.qi(schema)}.{self.qi(table)}"

    @abstractmethod
    def set_query_timeout(self, conn: Connection, seconds: int) -> None:
        """Set a per-query timeout for the current transaction/session."""

    def column_ddl(self, col: Column) -> str:
        """Build the DDL fragment for a single column."""
        type_str = col.sa_type.compile(dialect=self._sa_dialect)
        parts = [f"{self.qi(col.name)} {type_str}"]
        if not col.nullable:
            parts.append("NOT NULL")
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")
        return " ".join(parts)

    @functools.cached_property
    def _sa_dialect(self):
        """Return the SQLAlchemy dialect instance for type compilation."""
        raise NotImplementedError

    def adjust_pk_columns(
        self, columns: list[Column], pk_set: set[str],
    ) -> list[Column]:
        """Adjust column types for primary key compatibility.

        Override in dialects where certain types (e.g., TEXT) cannot be
        used in primary keys.  Default: return columns unchanged.
        """
        return columns

    def prepare_dataframe(self, df):
        """Prepare a DataFrame for writing to this backend.

        May mutate ``df`` in place — caller must pass an owned copy.
        Default: return DataFrame unchanged.
        """
        return df

    @abstractmethod
    def create_table_kw(self, unlogged: bool) -> str:
        """Return the CREATE keyword (e.g., 'CREATE UNLOGGED TABLE')."""

    @abstractmethod
    def delete_all_rows(
        self, conn: Connection, schema: str, table: str,
    ) -> None:
        """Remove all rows from a table atomically with the surrounding txn.

        PostgreSQL uses TRUNCATE (MVCC-safe, transactional). MariaDB cannot
        use TRUNCATE here: it is DDL and triggers an implicit commit, which
        defeats the transaction wrapper — so MariaDB uses DELETE instead
        (transactional on InnoDB).
        """

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
        column_types: dict[str, object] | None = None,
    ) -> str:
        """Build CREATE INDEX DDL for this dialect."""

    @abstractmethod
    def drop_table_ddl(self, schema: str, table_name: str) -> str:
        """Build DROP TABLE DDL for this dialect."""


class PostgresDialect(Dialect):
    """PostgreSQL 16+ dialect."""

    @functools.cached_property
    def _sa_dialect(self):
        from sqlalchemy.dialects import postgresql
        return postgresql.dialect()

    def qi(self, name: str) -> str:
        return f'"{name}"'

    def set_query_timeout(self, conn: Connection, seconds: int) -> None:
        conn.execute(text(f"SET LOCAL statement_timeout = '{seconds}s'"))

    def create_table_kw(self, unlogged: bool) -> str:
        return "CREATE UNLOGGED TABLE" if unlogged else "CREATE TABLE"

    def delete_all_rows(
        self, conn: Connection, schema: str, table: str,
    ) -> None:
        conn.execute(text(f"TRUNCATE TABLE {self.fqn(schema, table)}"))

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
        column_types: dict[str, object] | None = None,
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

    @functools.cached_property
    def _sa_dialect(self):
        from sqlalchemy.dialects import mysql
        return mysql.dialect()

    def qi(self, name: str) -> str:
        return f'`{name}`'

    def set_query_timeout(self, conn: Connection, seconds: int) -> None:
        conn.execute(text(f"SET max_statement_time = {seconds}"))

    def adjust_pk_columns(
        self, columns: list[Column], pk_set: set[str],
    ) -> list[Column]:
        """MariaDB cannot use TEXT columns in primary keys — convert to VARCHAR(255)."""
        from sqlalchemy import String, Text as SAText

        adjusted = []
        for c in columns:
            if c.name in pk_set and isinstance(c.sa_type, SAText):
                adjusted.append(Column(c.name, String(255), nullable=c.nullable, default=c.default))
            else:
                adjusted.append(c)
        return adjusted

    def prepare_dataframe(self, df):
        """Strip timezone info from datetime columns for MariaDB DATETIME.

        Mutates ``df`` in place — caller must pass an owned copy.
        """
        for col in df.columns:
            if hasattr(df[col].dtype, "tz") and df[col].dtype.tz is not None:
                df[col] = df[col].dt.tz_localize(None)
        return df

    def create_table_kw(self, unlogged: bool) -> str:
        # MariaDB has no UNLOGGED tables — use regular CREATE TABLE
        if unlogged:
            logger.debug("MariaDB: UNLOGGED not supported, using regular table")
        return "CREATE TABLE"

    def delete_all_rows(
        self, conn: Connection, schema: str, table: str,
    ) -> None:
        conn.execute(text(f"DELETE FROM {self.fqn(schema, table)}"))

    def dedup_temp_table(
        self, conn: Connection, schema: str, table: str, pk_cols: list[str],
    ) -> int:
        # MariaDB has no ctid — dedup via a TEMPORARY table shuffle. CREATE
        # and DROP TEMPORARY are the documented exceptions to MariaDB's
        # DDL-implicit-commit rule, so the whole flow stays inside the txn.
        cols_sql = ", ".join(f'`{c}`' for c in pk_cols)
        dedup_tbl = f"_dedup_{table}"

        # Idempotency guard: TEMPORARY tables survive txn rollback, so a
        # stale _dedup_<t> from a prior @db_retry attempt on the same
        # pooled connection must be dropped before CREATE.
        conn.execute(text(f"DROP TEMPORARY TABLE IF EXISTS `{dedup_tbl}`"))

        before = conn.execute(text(
            f"SELECT COUNT(*) FROM `{schema}`.`{table}`"
        )).scalar()

        conn.execute(text(
            f"CREATE TEMPORARY TABLE `{dedup_tbl}` AS "
            f"SELECT * FROM `{schema}`.`{table}` GROUP BY {cols_sql}"
        ))

        self.delete_all_rows(conn, schema, table)

        conn.execute(text(
            f"INSERT INTO `{schema}`.`{table}` SELECT * FROM `{dedup_tbl}`"
        ))

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
        column_types: dict[str, object] | None = None,
    ) -> str:
        name = index_name(table_name, idx)
        unique = "UNIQUE " if idx.unique else ""

        # For TEXT/BLOB columns, add a prefix length for indexability
        col_parts = []
        for c in idx.columns:
            col_str = f'`{c}`'
            if column_types:
                from sqlalchemy import Text as SAText
                ct = column_types.get(c)
                if isinstance(ct, SAText):
                    col_str = f'`{c}`(255)'
            col_parts.append(col_str)
        cols = ", ".join(col_parts)

        method = idx.method
        if method not in _MARIADB_INDEX_METHODS:
            logger.warning(
                "MariaDB does not support index method '%s' — falling back to BTREE "
                "for index '%s' on %s.%s",
                method.value, name, schema, table_name,
            )
            method = IndexMethod.BTREE

        # MariaDB syntax: USING goes between index name and ON keyword
        ddl = (
            f'CREATE {unique}INDEX IF NOT EXISTS `{name}` '
            f'USING {method} '
            f'ON `{schema}`.`{table_name}` ({cols})'
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
