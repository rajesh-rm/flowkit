"""Column definition for asset target table schemas."""

from __future__ import annotations

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
