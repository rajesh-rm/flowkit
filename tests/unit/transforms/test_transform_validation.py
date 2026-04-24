"""Validate transform assets against their source dependencies.

These tests run at CI time (no database required) and catch common
errors before a transform reaches production:

- Source tables must exist as registered assets
- SQL must reference the correct schema.table names
- SQL column references must exist in source asset definitions
- Declared output columns must match SQL SELECT aliases
"""

from __future__ import annotations

import re
from uuid import UUID

import pytest

from data_assets.core.column import Column, Index
from data_assets.core.enums import RunMode
from data_assets.core.registry import all_assets, discover
from data_assets.core.run_context import RunContext
from data_assets.core.transform_asset import TransformAsset
from data_assets.db.dialect import PostgresDialect
from sqlalchemy import Text


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_transforms() -> list[tuple[str, TransformAsset]]:
    """Return all registered transform assets as (name, instance) pairs."""
    discover()
    result = []
    for name, cls in sorted(all_assets().items()):
        asset = cls()
        if isinstance(asset, TransformAsset):
            result.append((name, asset))
    return result


def _build_source_catalog() -> dict[str, dict[str, str]]:
    """Build a lookup of schema.table -> {column_name: pg_type}.

    Uses all registered assets to map their target_schema.target_table
    to their column definitions.
    """
    catalog: dict[str, dict[str, str]] = {}
    for _name, cls in all_assets().items():
        asset = cls()
        fqn = f"{asset.target_schema}.{asset.target_table}"
        catalog[fqn] = {col.name: col.pg_type for col in asset.columns}
    return catalog


def _extract_table_refs(sql: str) -> list[str]:
    """Extract schema-qualified table references from SQL.

    Finds patterns like: FROM raw.servicenow_incidents
                         JOIN mart.some_table
    """
    pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)'
    return re.findall(pattern, sql, re.IGNORECASE)


def _extract_select_aliases(sql: str) -> list[str]:
    """Extract output column names from SELECT ... FROM.

    Uses the LAST SELECT-FROM pair so multi-CTE queries (including
    RECURSIVE CTEs) resolve to the final output SELECT, not the first
    CTE body. Splits on top-level commas (ignoring commas inside
    parentheses), then takes the AS alias or trailing identifier.
    """
    matches = list(re.finditer(
        r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL,
    ))
    if not matches:
        return []

    # Split on commas that are NOT inside parentheses
    select_body = matches[-1].group(1)
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in select_body:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    parts.append("".join(current).strip())

    aliases = []
    for part in parts:
        as_match = re.search(r'\bAS\s+(\w+)\s*$', part, re.IGNORECASE)
        if as_match:
            aliases.append(as_match.group(1).lower())
        else:
            word_match = re.search(r'(\w+)\s*$', part)
            if word_match:
                aliases.append(word_match.group(1).lower())
    return aliases


# ---------------------------------------------------------------------------
# Dummy context for calling query()
# ---------------------------------------------------------------------------

_DUMMY_CONTEXT = RunContext(
    run_id=UUID(int=0),
    mode=RunMode.TRANSFORM,
    asset_name="test",
)

_DUMMY_DIALECT = PostgresDialect()


# ---------------------------------------------------------------------------
# Tests — parametrized over all transform assets
# ---------------------------------------------------------------------------

_transforms = _get_transforms()

if not _transforms:
    pytest.skip("No transform assets registered", allow_module_level=True)


@pytest.mark.parametrize("name,asset", _transforms, ids=[t[0] for t in _transforms])
class TestTransformValidation:

    def test_source_tables_exist(self, name, asset):
        """Every source_table must match a registered asset's target_table."""
        known_tables = {cls().target_table for cls in all_assets().values()}
        for table in asset.source_tables:
            assert table in known_tables, (
                f"Transform '{name}' declares source_table '{table}' which "
                f"doesn't match any registered asset's target_table. "
                f"Known: {sorted(known_tables)}"
            )

    def test_sql_references_valid_tables(self, name, asset):
        """SQL FROM/JOIN clauses must reference schema.table of source assets."""
        catalog = _build_source_catalog()
        sql = asset.query(_DUMMY_CONTEXT, _DUMMY_DIALECT)
        table_refs = _extract_table_refs(sql)

        assert table_refs, (
            f"Transform '{name}': could not find any schema.table references "
            f"in query(). Use fully-qualified names like 'raw.my_table'."
        )

        for ref in table_refs:
            assert ref in catalog, (
                f"Transform '{name}' references '{ref}' in SQL but no "
                f"registered asset has target_schema.target_table = '{ref}'. "
                f"Known: {sorted(catalog.keys())}"
            )

    def test_source_columns_exist(self, name, asset):
        """Columns referenced in SQL should exist in source asset definitions."""
        catalog = _build_source_catalog()
        sql = asset.query(_DUMMY_CONTEXT, _DUMMY_DIALECT)
        table_refs = _extract_table_refs(sql)

        # For each referenced table, check column references
        for fqn in table_refs:
            if fqn not in catalog:
                continue  # Caught by test_sql_references_valid_tables
            source_cols = catalog[fqn]
            table_name = fqn.split(".")[-1]
            col_refs = re.findall(
                rf'\b{re.escape(table_name)}\.(\w+)\b', sql, re.IGNORECASE
            )
            # Also match schema.table.column
            col_refs += re.findall(
                rf'\b{re.escape(fqn)}\.(\w+)\b', sql, re.IGNORECASE
            )
            for col in col_refs:
                assert col in source_cols, (
                    f"Transform '{name}' references column '{fqn}.{col}' "
                    f"but source asset has no column '{col}'. "
                    f"Available: {sorted(source_cols.keys())}"
                )

    def test_output_columns_match_declared(self, name, asset):
        """SELECT aliases in query() must match the asset's declared columns."""
        sql = asset.query(_DUMMY_CONTEXT, _DUMMY_DIALECT)
        sql_aliases = _extract_select_aliases(sql)
        declared_cols = [c.name.lower() for c in asset.columns]

        assert sql_aliases == declared_cols, (
            f"Transform '{name}': SELECT aliases {sql_aliases} don't match "
            f"declared columns {declared_cols}. Check column order and AS aliases."
        )

    def test_source_tables_not_empty(self, name, asset):
        """Transform assets must declare at least one source table."""
        assert asset.source_tables, (
            f"Transform '{name}' has empty source_tables. "
            f"Declare the upstream tables this transform reads from."
        )


# ---------------------------------------------------------------------------
# Registry-level validation tests
# ---------------------------------------------------------------------------

class TestRegistryTransformValidation:

    def test_missing_source_table_raises(self):
        """Registry should raise ValueError for unknown source_tables."""
        from data_assets.core.registry import _registry, _validate_dependencies

        class _BadTransform(TransformAsset):
            name = "bad_transform"
            target_table = "bad_transform"
            source_tables = ["nonexistent_table"]
            columns = [Column("id", Text())]
            primary_key = ["id"]
            indexes = [Index(columns=("id",))]

            def query(self, context, dialect):
                return "SELECT 1 AS id"

        _registry["bad_transform"] = _BadTransform
        try:
            with pytest.raises(ValueError, match="nonexistent_table"):
                _validate_dependencies()
        finally:
            del _registry["bad_transform"]

    def test_circular_dependency_raises(self):
        """Registry should raise ValueError for circular source_tables."""
        from data_assets.core.registry import _registry, _validate_dependencies

        class _TransformA(TransformAsset):
            name = "cycle_a"
            target_table = "cycle_a"
            source_tables = ["cycle_b"]
            columns = [Column("id", Text())]
            primary_key = ["id"]
            indexes = [Index(columns=("id",))]

            def query(self, context, dialect):
                return "SELECT 1 AS id"

        class _TransformB(TransformAsset):
            name = "cycle_b"
            target_table = "cycle_b"
            source_tables = ["cycle_a"]
            columns = [Column("id", Text())]
            primary_key = ["id"]
            indexes = [Index(columns=("id",))]

            def query(self, context, dialect):
                return "SELECT 1 AS id"

        _registry["cycle_a"] = _TransformA
        _registry["cycle_b"] = _TransformB
        try:
            with pytest.raises(ValueError, match="Circular dependency"):
                _validate_dependencies()
        finally:
            del _registry["cycle_a"]
            del _registry["cycle_b"]
