"""Dialect-portable helpers for integration test assertions.

Always prefer these over raw ``pd.read_sql("SELECT ...")`` because:

1. SQLAlchemy Core auto-quotes identifiers per-dialect (backticks for
   MariaDB, double-quotes for Postgres). No more "KEY is a MariaDB
   reserved word" syntax errors or silent ``ORDER BY "key"``
   sort-on-literal-string bugs.
2. :func:`table_exists` uses SQLAlchemy ``inspect()`` — works on both
   backends without Postgres-only ``to_regclass()`` or dialect-specific
   ``information_schema`` queries.
3. The same assertion runs unchanged on Postgres and MariaDB — which
   matters because ``tests/conftest.py::db_engine`` parametrises every
   integration test over both backends by default.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import MetaData, Table, inspect, select
from sqlalchemy.engine import Engine


def table_exists(engine: Engine, schema: str, table: str) -> bool:
    """Return True if ``schema.table`` exists in the connected database.

    Example:
        >>> if table_exists(run_engine, "raw", "sonarqube_projects"):
        ...     df = read_rows(run_engine, "raw", "sonarqube_projects")
        ...     assert len(df) == 0
    """
    return inspect(engine).has_table(table, schema=schema)


def read_rows(
    engine: Engine,
    schema: str,
    table: str,
    where: dict | None = None,
    order_by: list[str] | None = None,
) -> pd.DataFrame:
    """Read rows from ``schema.table`` as a DataFrame, dialect-agnostically.

    Args:
        engine:    SQLAlchemy engine (typically from the ``run_engine`` or
                   ``clean_db`` fixture).
        schema:    Schema name, e.g. ``"raw"``, ``"mart"``, ``"data_ops"``.
        table:     Table name.
        where:     Equality filters as a dict, e.g. ``{"key": "issue-1"}``.
                   Column names may be SQL reserved words — SQLAlchemy
                   quotes them correctly for each backend.
        order_by:  List of column names, applied in the given order.

    Examples:
        All rows, ordered by the (MariaDB-reserved!) column ``key``::

            df = read_rows(engine, "raw", "sonarqube_projects",
                           order_by=["key"])

        Filter by a reserved word::

            df = read_rows(engine, "raw", "sonarqube_issues",
                           where={"key": "issue-1"})

        Run-history lookup::

            df = read_rows(engine, "data_ops", "run_history",
                           where={"asset_name": "sonarqube_projects"})
    """
    md = MetaData()
    t = Table(table, md, autoload_with=engine, schema=schema)
    stmt = select(t)
    if where:
        stmt = stmt.where(*[t.c[k] == v for k, v in where.items()])
    if order_by:
        stmt = stmt.order_by(*[t.c[c] for c in order_by])
    return pd.read_sql(stmt, engine)
