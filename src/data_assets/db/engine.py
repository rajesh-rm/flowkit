"""SQLAlchemy engine factory with connection pooling.

Connection string is resolved via the CredentialResolver order:
1. Airflow Connections (if airflow is installed)
2. Environment variable DATABASE_URL
3. .env file
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _resolve_database_url(connection_key: str = "data_assets_db") -> str:
    """Resolve the Postgres connection string from available sources."""
    # 1. Try Airflow Connection
    try:
        from airflow.hooks.base import BaseHook

        conn = BaseHook.get_connection(connection_key)
        return conn.get_uri()
    except ImportError:
        logger.debug("Airflow not installed, skipping connection lookup")
    except Exception:
        logger.warning(
            "Airflow connection '%s' lookup failed, falling back to env vars",
            connection_key, exc_info=True,
        )

    # 2. Environment variable or .env file
    load_dotenv()  # no-op if vars already set; loads .env otherwise
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
