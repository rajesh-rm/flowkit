"""SQLAlchemy engine factory with connection pooling.

Connection string is resolved via the CredentialResolver order:
1. Airflow Connections (if airflow is installed)
2. Environment variable DATABASE_URL
3. .env file
"""

from __future__ import annotations

import os
from functools import lru_cache

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def _resolve_database_url(connection_key: str = "data_assets_db") -> str:
    """Resolve the Postgres connection string from available sources."""
    # 1. Try Airflow Connection
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(connection_key)
        return conn.get_uri()
    except Exception:
        pass

    # 2. Environment variable
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    # 3. .env file
    load_dotenv()
    url = os.environ.get("DATABASE_URL")
    if url:
        return url

    raise RuntimeError(
        "No database connection found. Set DATABASE_URL environment variable, "
        "configure an Airflow Connection, or add DATABASE_URL to a .env file."
    )


@lru_cache(maxsize=1)
def get_engine(connection_key: str = "data_assets_db") -> Engine:
    """Create or return a cached SQLAlchemy engine with connection pooling."""
    url = _resolve_database_url(connection_key)
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )


def ensure_schemas(engine: Engine) -> None:
    """Create the required Postgres schemas if they don't exist."""
    schemas = ["data_ops", "raw", "mart", "temp_store"]
    with engine.begin() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
