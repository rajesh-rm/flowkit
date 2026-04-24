"""SQLAlchemy engine factory with connection pooling.

Connection string is resolved via the CredentialResolver order:
1. Airflow Connections (if airflow is installed)
2. Environment variable DATABASE_URL
3. .env file

Backend detection:
- Explicit: DATABASE_BACKEND=postgres|mariadb
- Auto-detected from DATABASE_URL prefix (postgresql:// vs mysql://)
- Runtime error if both are set and conflict
"""

from __future__ import annotations

import logging
import os
from functools import lru_cache

from dotenv import load_dotenv
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

SUPPORTED_BACKENDS = {"postgres", "mariadb"}

_BACKEND_FROM_URI = {
    "postgresql": "postgres",
    "postgres": "postgres",
    "mysql": "mariadb",
    "mariadb": "mariadb",
    "mysql+pymysql": "mariadb",
}


def _resolve_database_url(connection_key: str = "data_assets_db") -> str:
    """Resolve the database connection string from available sources."""
    # 1. Try Airflow Connection
    try:
        from airflow.sdk import BaseHook

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


def resolve_backend(url: str | None = None) -> str:
    """Determine the database backend from DATABASE_BACKEND and/or DATABASE_URL.

    Returns "postgres" or "mariadb".
    Raises RuntimeError if DATABASE_BACKEND conflicts with DATABASE_URL prefix.
    """
    explicit = os.environ.get("DATABASE_BACKEND", "").lower().strip()

    if url is None:
        try:
            url = _resolve_database_url()
        except RuntimeError:
            url = ""

    # Auto-detect from URI prefix
    detected = ""
    for prefix, backend in _BACKEND_FROM_URI.items():
        if url.startswith(f"{prefix}://") or url.startswith(f"{prefix}+"):
            detected = backend
            break

    if explicit and detected and explicit != detected:
        raise RuntimeError(
            f"DATABASE_BACKEND='{explicit}' conflicts with DATABASE_URL "
            f"which uses '{detected}'. Set one or the other, not both "
            f"with different values."
        )

    result = explicit or detected
    if not result:
        raise RuntimeError(
            "Cannot determine database backend. Set DATABASE_BACKEND=postgres "
            "or DATABASE_BACKEND=mariadb, or use a DATABASE_URL that starts "
            "with postgresql:// or mysql://."
        )

    if result not in SUPPORTED_BACKENDS:
        raise RuntimeError(
            f"Unsupported DATABASE_BACKEND='{result}'. "
            f"Supported: {sorted(SUPPORTED_BACKENDS)}"
        )

    return result


def attach_utc_session_hook(engine: Engine) -> None:
    """Force every new DB connection to use UTC session time zone.

    Without this, ``NOW()`` / ``CURRENT_TIMESTAMP`` and session-TZ-sensitive
    functions like Postgres ``DATE_TRUNC('week', tstz)::date`` produce
    results that depend on the OS locale of the DB server, which is
    outside our control in customer deployments. Pinning to UTC gives
    every time-window transform a single deterministic baseline.

    Public API: test harnesses call this on testcontainer engines to
    match production session defaults.
    """
    from data_assets.db.dialect import get_dialect

    try:
        stmt = get_dialect(engine).UTC_SESSION_SQL
    except ValueError:
        # Unsupported dialect — other dialect-specific ops would already
        # raise before any query reaches this connection.
        return

    @event.listens_for(engine, "connect")
    def _set_utc(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(stmt)
        finally:
            cursor.close()


@lru_cache(maxsize=1)
def get_engine(connection_key: str = "data_assets_db") -> Engine:
    """Create or return a cached SQLAlchemy engine with connection pooling."""
    url = _resolve_database_url(connection_key)
    engine = create_engine(
        url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
    attach_utc_session_hook(engine)
    return engine


def ensure_schemas(engine: Engine) -> None:
    """Create the required database schemas if they don't exist.

    Both PostgreSQL and MariaDB support CREATE SCHEMA IF NOT EXISTS.
    In MariaDB, SCHEMA is a synonym for DATABASE.
    """
    schemas = ["data_ops", "raw", "mart", "temp_store"]
    with engine.begin() as conn:
        for schema in schemas:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
