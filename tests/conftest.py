"""Shared pytest fixtures for data_assets tests.

Uses testcontainers for a real Postgres instance in integration tests.
Unit tests use a lightweight in-memory approach where possible.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
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


# ---------------------------------------------------------------------------
# Postgres fixture (integration tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pg_engine():
    """Create a test Postgres engine via testcontainers.

    Falls back to DATABASE_URL env var if testcontainers/Docker is not available.
    """
    # Try testcontainers first (requires Docker or Podman)
    try:
        from testcontainers.postgres import PostgresContainer

        # Ensure the container runtime socket is discoverable
        socket_path = _find_docker_socket()
        if socket_path:
            # DOCKER_HOST needs the unix:// scheme (for docker-py)
            os.environ.setdefault("DOCKER_HOST", f"unix://{socket_path}")
            # TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE needs the bare path
            # (mounted as a volume into the Ryuk cleanup container)
            os.environ.setdefault(
                "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", socket_path,
            )

        # Podman doesn't support Ryuk (the cleanup sidecar)
        if "podman" in os.environ.get("DOCKER_HOST", ""):
            os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")

        with PostgresContainer("postgres:16-alpine") as pg:
            url = pg.get_connection_url()
            engine = create_engine(url)
            _setup_schemas(engine)
            yield engine
            return
    except Exception as exc:
        _container_error = str(exc)

    # Fallback to DATABASE_URL
    url = os.environ.get("DATABASE_URL")
    if url:
        engine = create_engine(url)
        _setup_schemas(engine)
        yield engine
        return

    pytest.skip(
        f"No Postgres available for integration tests.\n"
        f"  Container error: {_container_error}\n"
        f"Options:\n"
        f"  1. Start Docker Desktop (macOS) or enable podman.socket (RHEL)\n"
        f"  2. Set DATABASE_URL env var to an existing Postgres instance\n"
        f"  3. Set DOCKER_HOST to a custom Docker/Podman socket URI"
    )


def _setup_schemas(engine: Engine) -> None:
    """Create schemas and metadata tables for testing."""
    from data_assets.db.models import create_all_tables

    with engine.begin() as conn:
        for schema in ["data_ops", "raw", "mart", "temp_store"]:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
    create_all_tables(engine)


@pytest.fixture
def clean_db(pg_engine):
    """Clean all tables before each test, return the engine."""
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM data_ops.run_locks"))
        conn.execute(text("DELETE FROM data_ops.run_history"))
        conn.execute(text("DELETE FROM data_ops.checkpoints"))
        conn.execute(text("DELETE FROM data_ops.asset_registry"))
        conn.execute(text("DELETE FROM data_ops.coverage_tracker"))
        # Drop all tables in raw, mart, temp_store
        for schema in ["raw", "mart", "temp_store"]:
            tables = conn.execute(text(
                f"SELECT tablename FROM pg_tables WHERE schemaname = '{schema}'"
            )).fetchall()
            for (t,) in tables:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{t}" CASCADE'))
    return pg_engine


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
