"""Shared pytest fixtures for data_assets tests.

Uses testcontainers for a real Postgres instance in integration tests.
Unit tests use a lightweight in-memory approach where possible.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Postgres fixture (integration tests)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def pg_engine():
    """Create a test Postgres engine via testcontainers.

    Falls back to DATABASE_URL env var if testcontainers is not available.
    """
    try:
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer("postgres:16-alpine") as pg:
            url = pg.get_connection_url()
            engine = create_engine(url)
            _setup_schemas(engine)
            yield engine
    except ImportError:
        url = os.environ.get("DATABASE_URL")
        if not url:
            pytest.skip("No Postgres available (install testcontainers or set DATABASE_URL)")
        engine = create_engine(url)
        _setup_schemas(engine)
        yield engine


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


@pytest.fixture
def run_id():
    """Generate a fresh UUID for each test."""
    return uuid.uuid4()


@pytest.fixture
def mock_env(monkeypatch):
    """Return a helper to set environment variables for testing."""
    def _set(**kwargs):
        for key, value in kwargs.items():
            monkeypatch.setenv(key, value)
    return _set


# ---------------------------------------------------------------------------
# Unit test fixtures (no DB required)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_run_context(run_id):
    """A basic RunContext for unit tests."""
    from data_assets.core.enums import RunMode
    from data_assets.core.run_context import RunContext

    return RunContext(
        run_id=run_id,
        mode=RunMode.FULL,
        asset_name="test_asset",
        start_date=None,
        end_date=None,
        params={},
    )
