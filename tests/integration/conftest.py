"""Shared fixtures for integration tests — eliminates boilerplate."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import patch

import pandas as pd
import pytest

from data_assets.extract.token_manager import TokenManager


# ---------------------------------------------------------------------------
# Dialect gating — skip @postgres_only / @mariadb_only on the wrong backend
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _dialect_gate(request, db_engine):
    """Skip tests marked @pytest.mark.postgres_only (or mariadb_only) when
    running against the other backend.

    Use sparingly — prefer dialect-portable assertions via the helpers in
    :mod:`tests.integration._db_utils`. A marker is only correct when the
    test genuinely depends on a feature that one backend does not support
    (e.g., ``CREATE UNLOGGED TABLE`` or ``ctid``-based dedup).
    """
    name = db_engine.dialect.name
    if request.node.get_closest_marker("postgres_only") and name != "postgresql":
        pytest.skip("marked @pytest.mark.postgres_only")
    if request.node.get_closest_marker("mariadb_only") and name not in ("mysql", "mariadb"):
        pytest.skip("marked @pytest.mark.mariadb_only")


# ---------------------------------------------------------------------------
# Token manager patching
# ---------------------------------------------------------------------------


@contextmanager
def stub_token_manager(cls):
    """Patch a TokenManager subclass to skip real credential resolution."""
    with patch.object(cls, "__init__", lambda self: TokenManager.__init__(self)):
        with patch.object(cls, "get_token", return_value="test-token"):
            with patch.object(
                cls, "get_auth_header",
                return_value={"Authorization": "Bearer test-token"},
            ):
                yield


# ---------------------------------------------------------------------------
# Engine patching — redirect runner + db module to test engine
# ---------------------------------------------------------------------------


@pytest.fixture
def run_engine(clean_db):
    """Patch get_engine everywhere so run_asset() uses the test Postgres."""
    with patch("data_assets.runner.get_engine", return_value=clean_db):
        with patch("data_assets.db.engine.get_engine", return_value=clean_db):
            yield clean_db


# ---------------------------------------------------------------------------
# Table seeding helper
# ---------------------------------------------------------------------------


def seed_table(engine, schema: str, table: str, rows: list[dict]) -> None:
    """Insert rows into a table for test setup (e.g., parent tables)."""
    df = pd.DataFrame(rows)
    df.to_sql(table, engine, schema=schema, if_exists="replace", index=False)
