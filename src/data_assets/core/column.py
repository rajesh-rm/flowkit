"""Column and Index definitions for asset target table schemas."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from sqlalchemy import types as sa_types
from sqlalchemy.types import TypeEngine

from data_assets.core.enums import IndexMethod

# ---------------------------------------------------------------------------
# Backward-compatible mapping: pg_type string → SQLAlchemy type
# ---------------------------------------------------------------------------

_PG_TYPE_MAP: dict[str, TypeEngine] = {
    "TEXT": sa_types.Text(),
    "INTEGER": sa_types.Integer(),
    "BIGINT": sa_types.BigInteger(),
    "BOOLEAN": sa_types.Boolean(),
    "DATE": sa_types.Date(),
    "TIMESTAMPTZ": sa_types.DateTime(timezone=True),
    "TIMESTAMP": sa_types.DateTime(),
    "FLOAT": sa_types.Float(),
    "DOUBLE PRECISION": sa_types.Float(),
    "NUMERIC": sa_types.Numeric(),
    "JSON": sa_types.JSON(),
    "JSONB": sa_types.JSON(),
    "UUID": sa_types.Uuid(),
}


def _resolve_type(sa_type: TypeEngine | str | None, pg_type: str | None) -> TypeEngine:
    """Resolve a Column type to a SQLAlchemy TypeEngine.

    Accepts either a SQLAlchemy type object (preferred) or a legacy pg_type
    string (backward-compatible).
    """
    if sa_type is not None and isinstance(sa_type, TypeEngine):
        return sa_type

    if sa_type is not None and isinstance(sa_type, str):
        # Caller passed a string as sa_type — treat as pg_type
        pg_type = sa_type
        sa_type = None

    if pg_type is not None:
        key = pg_type.upper()
        resolved = _PG_TYPE_MAP.get(key)
        if resolved is not None:
            return resolved
        raise ValueError(
            f"Unknown column type '{pg_type}'. Use a SQLAlchemy type object "
            f"(e.g., Text(), Integer(), DateTime(timezone=True)) or one of "
            f"these legacy strings: {sorted(_PG_TYPE_MAP.keys())}"
        )

    raise ValueError("Column requires either sa_type or pg_type")


@dataclass(frozen=True)
class Column:
    """A single column in an asset's target table.

    Attributes:
        name: Column name.
        sa_type: SQLAlchemy type (e.g., Text(), Integer(), DateTime(timezone=True)).
            Also accepts legacy pg_type strings for backward compatibility.
        nullable: Whether the column allows NULLs.
        default: Optional SQL default expression (e.g. "now()").
    """

    name: str
    sa_type: TypeEngine | str = field(default_factory=sa_types.Text)
    nullable: bool = True
    default: str | None = None

    # --- Backward compatibility ---
    # Accept Column("name", "TEXT") or Column("name", Text())
    def __init__(
        self,
        name: str,
        sa_type: TypeEngine | str = None,
        nullable: bool = True,
        default: str | None = None,
        *,
        pg_type: str | None = None,
    ):
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "nullable", nullable)
        object.__setattr__(self, "default", default)
        resolved = _resolve_type(sa_type, pg_type)
        object.__setattr__(self, "sa_type", resolved)

    @property
    def pg_type(self) -> str:
        """Backward-compatible accessor. Returns the type as a Postgres string."""
        from sqlalchemy.dialects import postgresql
        return str(self.sa_type.compile(dialect=postgresql.dialect()))


@dataclass(frozen=True)
class Index:
    """A database index on an asset's target table.

    Every asset must declare at least one index. Multiple indexes can
    reference the same column (e.g., a plain btree and a partial index).

    Attributes:
        columns: Column names in the index (order matters for composites).
        unique: Whether this is a UNIQUE index.
        method: Index access method (IndexMethod enum). Default: BTREE.
        where: Optional partial index WHERE clause (raw SQL, omit the
            WHERE keyword). Example: "state = 'open'"
        include: Optional INCLUDE columns for covering indexes.
        name: Optional explicit name. Auto-generated if omitted as
            ix_{table}_{col1}_{col2}[_unique][_partial].
    """

    columns: tuple[str, ...]
    unique: bool = False
    method: IndexMethod = IndexMethod.BTREE
    where: str | None = None
    include: tuple[str, ...] | None = None
    name: str | None = None


MAX_IDENTIFIER_LENGTH = 63  # Postgres=63, MariaDB=64; use the smaller
PG_MAX_IDENTIFIER = MAX_IDENTIFIER_LENGTH  # backward-compat alias


def index_name(table: str, idx: Index) -> str:
    """Generate a deterministic index name from table and index definition.

    Convention: ix_{table}_{col1}_{col2}[_unique][_partial]
    Truncated to 63 chars (smallest DB limit) with a hash suffix if needed.
    """
    if idx.name:
        return idx.name
    parts = ["ix", table, *idx.columns]
    if idx.unique:
        parts.append("unique")
    if idx.where:
        parts.append("partial")
    candidate = "_".join(parts)
    if len(candidate) <= MAX_IDENTIFIER_LENGTH:
        return candidate
    # Truncate and append short hash for uniqueness.
    # usedforsecurity=False: this is a non-cryptographic identifier collision
    # suffix (DB index names), not a security primitive. Flagged otherwise by
    # static analyzers (SonarQube python:S4790).
    h = hashlib.md5(candidate.encode(), usedforsecurity=False).hexdigest()[:8]
    return candidate[: MAX_IDENTIFIER_LENGTH - 9] + "_" + h
