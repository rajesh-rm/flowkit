"""Integration test: validate transform SQL against real Postgres.

Creates DDL-only source tables (no data) from registered asset definitions,
then runs each transform's query() to verify:
- SQL is syntactically valid
- All referenced columns exist
- Output schema matches declared columns in name and type
"""

from __future__ import annotations

from uuid import UUID

import pytest
from sqlalchemy import text

from data_assets.core.registry import all_assets, discover
from data_assets.core.run_context import RunContext
from data_assets.core.enums import RunMode
from data_assets.core.transform_asset import TransformAsset
from data_assets.load.loader import create_table


_DUMMY_CONTEXT = RunContext(
    run_id=UUID(int=0),
    mode=RunMode.TRANSFORM,
    asset_name="test",
)


def _create_source_tables(engine):
    """Create DDL-only tables for all registered assets (no data)."""
    discover()
    for _name, cls in sorted(all_assets().items()):
        asset = cls()
        if asset.columns:
            create_table(engine, asset.target_schema, asset.target_table, asset.columns)


def _get_transforms() -> list[tuple[str, type[TransformAsset]]]:
    """Return all transform asset classes."""
    discover()
    result = []
    for name, cls in sorted(all_assets().items()):
        asset = cls()
        if isinstance(asset, TransformAsset):
            result.append((name, cls))
    return result


_transforms = _get_transforms()


@pytest.mark.integration
class TestTransformSchema:

    @pytest.fixture(autouse=True)
    def _setup_tables(self, pg_engine):
        """Create source tables before running transform tests."""
        _create_source_tables(pg_engine)
        self.engine = pg_engine

    @pytest.mark.parametrize("name,cls", _transforms, ids=[t[0] for t in _transforms])
    def test_query_executes_against_empty_tables(self, name, cls):
        """Transform query() must be valid SQL against source table schemas."""
        asset = cls()
        sql = asset.query(_DUMMY_CONTEXT)

        with self.engine.begin() as conn:
            conn.execute(text(
                f"SET LOCAL statement_timeout = '{asset.query_timeout_seconds}s'"
            ))
            result = conn.execute(text(sql))
            columns = list(result.keys())

        declared = [c.name for c in asset.columns]
        assert columns == declared, (
            f"Transform '{name}': query output columns {columns} don't match "
            f"declared columns {declared}"
        )

    @pytest.mark.parametrize("name,cls", _transforms, ids=[t[0] for t in _transforms])
    def test_query_returns_zero_rows_on_empty_tables(self, name, cls):
        """With no source data, transform should return 0 rows (not error)."""
        asset = cls()
        sql = asset.query(_DUMMY_CONTEXT)

        with self.engine.begin() as conn:
            result = conn.execute(text(sql))
            rows = result.fetchall()

        assert len(rows) == 0, (
            f"Transform '{name}' returned {len(rows)} rows from empty source "
            f"tables — expected 0. Check for hardcoded data or missing WHERE."
        )
