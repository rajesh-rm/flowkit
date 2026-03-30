"""SQLAlchemy ORM models for all data_ops metadata tables."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class RunLock(Base):
    """Mutex preventing concurrent runs of the same asset.

    A row exists only while a run is active.
    """

    __tablename__ = "run_locks"
    __table_args__ = {"schema": "data_ops"}

    asset_name: Mapped[str] = mapped_column(Text, primary_key=True)
    run_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    locked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    locked_by: Mapped[str] = mapped_column(Text, nullable=False)


class RunHistory(Base):
    """Append-only log of every completed run with metrics."""

    __tablename__ = "run_history"
    __table_args__ = {"schema": "data_ops"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    asset_name: Mapped[str] = mapped_column(Text, nullable=False)
    run_mode: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    rows_extracted: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rows_loaded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSONB, nullable=True)
    airflow_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)


class Checkpoint(Base):
    """Extraction progress for resumable runs.

    For parallel extraction, multiple rows per run (one per worker partition).
    """

    __tablename__ = "checkpoints"
    __table_args__ = {"schema": "data_ops"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    asset_name: Mapped[str] = mapped_column(Text, nullable=False)
    worker_id: Mapped[str] = mapped_column(Text, nullable=False, default="main")
    checkpoint_type: Mapped[str] = mapped_column(Text, nullable=False)
    checkpoint_value: Mapped[dict] = mapped_column(JSONB, nullable=False)
    rows_so_far: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="in_progress")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )


class AssetRegistry(Base):
    """Canonical list of assets known to the system."""

    __tablename__ = "asset_registry"
    __table_args__ = {"schema": "data_ops"}

    asset_name: Mapped[str] = mapped_column(Text, primary_key=True)
    asset_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_schema: Mapped[str] = mapped_column(Text, nullable=False)
    target_table: Mapped[str] = mapped_column(Text, nullable=False)
    load_strategy: Mapped[str] = mapped_column(Text, nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    config: Mapped[dict | None] = mapped_column(JSONB, nullable=True)


class CoverageTracker(Base):
    """Tracks data time boundaries each asset has successfully loaded."""

    __tablename__ = "coverage_tracker"
    __table_args__ = {"schema": "data_ops"}

    asset_name: Mapped[str] = mapped_column(Text, primary_key=True)
    forward_watermark: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    backward_watermark: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("now()"),
    )


def create_all_tables(engine) -> None:
    """Create all data_ops tables (idempotent)."""
    from data_assets.db.engine import ensure_schemas

    ensure_schemas(engine)
    Base.metadata.create_all(engine)
