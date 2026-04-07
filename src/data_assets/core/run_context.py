"""Immutable run context passed through the entire run lifecycle."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from uuid import UUID

from data_assets.core.enums import RunMode


@dataclass(frozen=True)
class RunContext:
    """Immutable snapshot of parameters for a single asset run.

    Created at the start of a run and threaded through every phase.
    """

    run_id: UUID
    mode: RunMode
    asset_name: str
    partition_key: str = ""
    start_date: datetime | None = None
    end_date: datetime | None = None
    params: dict = field(default_factory=dict)
