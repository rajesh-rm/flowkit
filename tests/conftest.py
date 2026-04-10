"""Shared pytest fixtures for data_assets tests.

Uses testcontainers for real database instances in integration tests.
Set TEST_DATABASE=mariadb to test against MariaDB (default: postgres).
Unit tests use a lightweight in-memory approach where possible.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from data_assets.core.registry import _registry
from data_assets.extract.token_manager import TokenManager

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Shared across unit + integration tests
# ---------------------------------------------------------------------------


class StubTokenManager(TokenManager):
    """Minimal token manager for testing — no real credentials needed."""

    def get_token(self) -> str:
        return "test-token"

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": "Bearer test-token"}


@pytest.fixture(autouse=True)
def _clean_registry():
    """Isolate the asset registry between tests.

    Saves the registry before each test, restores it after. This prevents
    ad-hoc @register calls in one test from leaking into the next.

    For integration tests that call run_asset() → discover(): the first
    test discovers all assets; subsequent tests reuse them because Python
    caches module imports (re-importing doesn't re-execute @register).
    We snapshot AFTER yield so the discovered state is preserved.
    """
    snapshot = dict(_registry)
    yield
    # If discover() ran during this test and the pre-test snapshot was
    # empty, keep the discovered assets — they can't be re-discovered.
    if _registry and not snapshot:
        return
    _registry.clear()
    _registry.update(snapshot)


# ---------------------------------------------------------------------------
# Container runtime socket discovery
# ---------------------------------------------------------------------------


def _find_docker_socket() -> str | None:
    """Detect a working Docker or Podman socket path for testcontainers.

    Checks in order of priority:
    1. DOCKER_HOST env var (explicit override — always wins)
    2. Podman rootless socket ($XDG_RUNTIME_DIR/podman/podman.sock)
    3. macOS Docker Desktop socket (~/.docker/run/docker.sock)
    4. Default /var/run/docker.sock (Linux Docker, or macOS symlink)

    Returns a bare socket path (e.g., /path/to/docker.sock) or None.
    """
    # 1. Explicit override — the user knows best
    if os.environ.get("DOCKER_HOST"):
        return None

    candidates: list[str] = []

    # 2. Podman rootless (RHEL, Fedora)
    xdg = os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}")
    candidates.append(f"{xdg}/podman/podman.sock")

    # 3. macOS Docker Desktop
    if sys.platform == "darwin":
        candidates.append(os.path.expanduser("~/.docker/run/docker.sock"))

    # 4. Default Linux Docker
    candidates.append("/var/run/docker.sock")

    for sock in candidates:
        if os.path.exists(sock):
            return sock

    return None


def _setup_container_runtime() -> None:
    """Configure Docker/Podman socket for testcontainers."""
    socket_path = _find_docker_socket()
    if socket_path:
        os.environ.setdefault("DOCKER_HOST", f"unix://{socket_path}")
        os.environ.setdefault(
            "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", socket_path,
        )
    if "podman" in os.environ.get("DOCKER_HOST", ""):
        os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")


# ---------------------------------------------------------------------------
# Database engine fixtures (integration tests)
# ---------------------------------------------------------------------------


def _test_database() -> str:
    """Return the test database backend: 'postgres' or 'mariadb'."""
    return os.environ.get("TEST_DATABASE", "postgres").lower().strip()


_DB_CONFIGS = {
    "postgres": {
        "import": "testcontainers.postgres",
        "class": "PostgresContainer",
        "image": "postgres:16-alpine",
        "label": "Postgres",
    },
    "mariadb": {
        "import": "testcontainers.mysql",
        "class": "MySqlContainer",
        "image": "mariadb:10.11",
        "label": "MariaDB",
        "kwargs": {"dialect": "pymysql"},
    },
}


def _create_db_engine(backend: str) -> tuple[Engine, object | None]:
    """Create a database engine via testcontainers or DATABASE_URL.

    Returns (engine, container_or_None).
    """
    cfg = _DB_CONFIGS[backend]
    try:
        import importlib
        mod = importlib.import_module(cfg["import"])
        container_cls = getattr(mod, cfg["class"])

        _setup_container_runtime()
        container = container_cls(cfg["image"], **cfg.get("kwargs", {}))
        container.start()
        url = container.get_connection_url()
        return create_engine(url), container
    except Exception as exc:
        url = os.environ.get("DATABASE_URL")
        if url:
            return create_engine(url), None
        pytest.skip(
            f"No {cfg['label']} available for integration tests.\n"
            f"  Container error: {exc}\n"
            f"Options:\n"
            f"  1. Start Docker Desktop (macOS) or enable podman.socket (RHEL)\n"
            f"  2. Set DATABASE_URL env var to a {cfg['label']} instance\n"
            f"  3. Set DOCKER_HOST to a custom Docker/Podman socket URI"
        )


def _setup_schemas(engine: Engine, container=None) -> None:
    """Create schemas and metadata tables for testing.

    For MariaDB, schema creation requires root access (CREATE DATABASE).
    We use the container's root credentials if available, then grant
    privileges to the regular user.
    """
    from data_assets.db.models import create_all_tables

    dialect = engine.dialect.name

    if dialect in ("mysql", "mariadb") and container is not None:
        # MariaDB: use root to create schemas and grant privileges
        root_url = (
            f"mysql+pymysql://root:{container.root_password}"
            f"@{container.get_container_host_ip()}"
            f":{container.get_exposed_port(3306)}/{container.dbname}"
        )
        root_engine = create_engine(root_url)
        with root_engine.begin() as conn:
            for schema in ["data_ops", "raw", "mart", "temp_store"]:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.execute(text(f"GRANT ALL PRIVILEGES ON *.* TO '{container.username}'@'%'"))
            conn.execute(text("FLUSH PRIVILEGES"))
        root_engine.dispose()
    else:
        with engine.begin() as conn:
            for schema in ["data_ops", "raw", "mart", "temp_store"]:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    create_all_tables(engine)


@pytest.fixture(scope="session")
def db_engine():
    """Create a test database engine via testcontainers.

    Respects TEST_DATABASE env var:
    - 'postgres' (default): PostgreSQL 16 via testcontainers
    - 'mariadb': MariaDB 10.11 via testcontainers
    """
    backend = _test_database()
    engine, container = _create_db_engine(backend)

    _setup_schemas(engine, container)
    yield engine

    if container:
        container.stop()


# Backward-compat alias — existing tests use pg_engine
pg_engine = db_engine


@pytest.fixture
def clean_db(db_engine):
    """Clean all tables before each test, return the engine.

    Uses SQLAlchemy inspect() for dialect-agnostic table discovery
    and dialect.drop_table_ddl() for dialect-correct DROP TABLE.
    """
    from data_assets.db.dialect import get_dialect

    d = get_dialect(db_engine)
    insp = inspect(db_engine)
    with db_engine.begin() as conn:
        conn.execute(text("DELETE FROM data_ops.run_locks"))
        conn.execute(text("DELETE FROM data_ops.run_history"))
        conn.execute(text("DELETE FROM data_ops.checkpoints"))
        conn.execute(text("DELETE FROM data_ops.asset_registry"))
        conn.execute(text("DELETE FROM data_ops.coverage_tracker"))
        for schema in ["raw", "mart", "temp_store"]:
            for table_name in insp.get_table_names(schema=schema):
                conn.execute(text(d.drop_table_ddl(schema, table_name)))
    return db_engine


# ---------------------------------------------------------------------------
# Fixture data loaders
# ---------------------------------------------------------------------------

@pytest.fixture
def load_fixture():
    """Return a callable that loads JSON fixture data."""
    def _load(relative_path: str) -> dict | list:
        path = FIXTURES_DIR / relative_path
        return json.loads(path.read_text())
    return _load
