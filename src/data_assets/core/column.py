"""Column and Index definitions for asset target table schemas."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass(frozen=True)
class Column:
    """A single column in an asset's target table.

    Attributes:
        name: Column name in Postgres.
        pg_type: Postgres type string (e.g. "TEXT", "INTEGER", "TIMESTAMPTZ").
        nullable: Whether the column allows NULLs.
        default: Optional SQL default expression (e.g. "now()").
    """

    name: str
    pg_type: str
    nullable: bool = True
    default: str | None = None


@dataclass(frozen=True)
class Index:
    """A database index on an asset's target table.

    Every asset must declare at least one index. Multiple indexes can
    reference the same column (e.g., a plain btree and a partial index).

    Attributes:
        columns: Column names in the index (order matters for composites).
        unique: Whether this is a UNIQUE index.
        method: Index access method — "btree" (default), "gin", "hash".
        where: Optional partial index WHERE clause (raw SQL, omit the
            WHERE keyword). Example: "state = 'open'"
        include: Optional INCLUDE columns for covering indexes.
        name: Optional explicit name. Auto-generated if omitted as
            ix_{table}_{col1}_{col2}[_unique][_partial].
    """

    columns: tuple[str, ...]
    unique: bool = False
    method: str = "btree"
    where: str | None = None
    include: tuple[str, ...] | None = None
    name: str | None = None


PG_MAX_IDENTIFIER = 63


def index_name(table: str, idx: Index) -> str:
    """Generate a deterministic index name from table and index definition.

    Convention: ix_{table}_{col1}_{col2}[_unique][_partial]
    Truncated to 63 chars (Postgres limit) with a hash suffix if needed.
    """
    if idx.name:
        return idx.name
    parts = ["ix", table, *idx.columns]
    if idx.unique:
        parts.append("unique")
    if idx.where:
        parts.append("partial")
    candidate = "_".join(parts)
    if len(candidate) <= PG_MAX_IDENTIFIER:
        return candidate
    # Truncate and append short hash for uniqueness
    h = hashlib.md5(candidate.encode()).hexdigest()[:8]
    return candidate[: PG_MAX_IDENTIFIER - 9] + "_" + h
