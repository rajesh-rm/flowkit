"""SQLAlchemy ORM models for all data_ops metadata tables.

All types are database-agnostic (no dialect-specific imports).
Supports PostgreSQL 16+ and MariaDB 10.11+.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, Integer, JSON, String, Text, Uuid, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Portable server default for timestamps — works on both Postgres and MariaDB.
# func.now() generates DEFAULT (now()) which MariaDB rejects for DATETIME.
# CURRENT_TIMESTAMP is SQL standard and accepted by both.
_CURRENT_TIMESTAMP = text("CURRENT_TIMESTAMP")


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class RunLock(Base):
    """Mutex preventing concurrent runs of the same asset partition.

    A row exists only while a run is active.  Also tracks the temp table
    and heartbeat so a retry worker can detect abandoned runs and take
    over their partial work.

    The composite PK (asset_name, partition_key) allows multi-org runs
    to hold independent locks on the same asset. partition_key defaults
    to "" for non-partitioned assets.
    """

    __tablename__ = "run_locks"
    __table_args__ = {"schema": "data_ops"}

    asset_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    partition_key: Mapped[str] = mapped_column(String(255), primary_key=True, default="")
    run_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    locked_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_CURRENT_TIMESTAMP,
    )
    locked_by: Mapped[str] = mapped_column(Text, nullable=False)
    temp_table: Mapped[str | None] = mapped_column(Text, nullable=True)
    heartbeat_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_CURRENT_TIMESTAMP,
    )


class RunHistory(Base):
    """Append-only log of every completed run with metrics."""

    __tablename__ = "run_history"
    __table_args__ = {"schema": "data_ops"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    asset_name: Mapped[str] = mapped_column(Text, nullable=False)
    partition_key: Mapped[str] = mapped_column(Text, nullable=False, default="")
    run_mode: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    rows_extracted: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rows_loaded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_: Mapped[dict | None] = mapped_column("metadata", JSON, nullable=True)
    airflow_run_id: Mapped[str | None] = mapped_column(Text, nullable=True)


class Checkpoint(Base):
    """Extraction progress for resumable runs.

    For parallel extraction, multiple rows per run (one per worker partition).
    """

    __tablename__ = "checkpoints"
    __table_args__ = {"schema": "data_ops"}

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    asset_name: Mapped[str] = mapped_column(Text, nullable=False)
    partition_key: Mapped[str] = mapped_column(Text, nullable=False, default="")
    worker_id: Mapped[str] = mapped_column(Text, nullable=False, default="main")
    checkpoint_type: Mapped[str] = mapped_column(Text, nullable=False)
    checkpoint_value: Mapped[dict] = mapped_column(JSON, nullable=False)
    rows_so_far: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="in_progress")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_CURRENT_TIMESTAMP,
    )


class AssetRegistry(Base):
    """Canonical list of assets known to the system."""

    __tablename__ = "asset_registry"
    __table_args__ = {"schema": "data_ops"}

    asset_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    asset_type: Mapped[str] = mapped_column(Text, nullable=False)
    source_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_schema: Mapped[str] = mapped_column(Text, nullable=False)
    target_table: Mapped[str] = mapped_column(Text, nullable=False)
    load_strategy: Mapped[str] = mapped_column(Text, nullable=False)
    registered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_CURRENT_TIMESTAMP,
    )
    last_success_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    config: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class CoverageTracker(Base):
    """Tracks data time boundaries each asset partition has successfully loaded.

    The composite PK (asset_name, partition_key) allows multi-org runs
    to maintain independent watermarks. partition_key defaults to ""
    for non-partitioned assets.
    """

    __tablename__ = "coverage_tracker"
    __table_args__ = {"schema": "data_ops"}

    asset_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    partition_key: Mapped[str] = mapped_column(String(255), primary_key=True, default="")
    forward_watermark: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    backward_watermark: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=_CURRENT_TIMESTAMP,
    )


def _migrate_add_partition_key(engine) -> None:
    """One-time migration: add partition_key column to operational tables.

    Safe to call multiple times (idempotent). Existing rows get
    partition_key="" from the DEFAULT, preserving backward compatibility.

    On a fresh install where create_all() already defined the composite PKs,
    the PK rebuild steps are no-ops (caught by try/except).
    """
    import logging
    from sqlalchemy import text

    logger = logging.getLogger(__name__)

    # Step 1: Add partition_key column (idempotent via IF NOT EXISTS)
    add_col_stmts = [
        "ALTER TABLE data_ops.run_locks "
        "ADD COLUMN IF NOT EXISTS partition_key VARCHAR(255) NOT NULL DEFAULT ''",
        "ALTER TABLE data_ops.coverage_tracker "
        "ADD COLUMN IF NOT EXISTS partition_key VARCHAR(255) NOT NULL DEFAULT ''",
        "ALTER TABLE data_ops.checkpoints "
        "ADD COLUMN IF NOT EXISTS partition_key VARCHAR(255) NOT NULL DEFAULT ''",
        "ALTER TABLE data_ops.run_history "
        "ADD COLUMN IF NOT EXISTS partition_key VARCHAR(255) NOT NULL DEFAULT ''",
    ]

    with engine.begin() as conn:
        for sql in add_col_stmts:
            conn.execute(text(sql))

    # Step 2: Rebuild composite PKs (may already exist from create_all)
    pk_stmts = [
        ("ALTER TABLE data_ops.run_locks DROP CONSTRAINT IF EXISTS run_locks_pkey",
         "ALTER TABLE data_ops.run_locks ADD CONSTRAINT run_locks_pkey "
         "PRIMARY KEY (asset_name, partition_key)"),
        ("ALTER TABLE data_ops.coverage_tracker DROP CONSTRAINT IF EXISTS coverage_tracker_pkey",
         "ALTER TABLE data_ops.coverage_tracker ADD CONSTRAINT coverage_tracker_pkey "
         "PRIMARY KEY (asset_name, partition_key)"),
    ]

    for drop_sql, add_sql in pk_stmts:
        try:
            with engine.begin() as conn:
                conn.execute(text(drop_sql))
                conn.execute(text(add_sql))
        except Exception:
            # PK already correct (fresh install via create_all)
            logger.debug("PK already exists, skipping rebuild")


def create_all_tables(engine) -> None:
    """Create all data_ops tables (idempotent)."""
    from data_assets.db.engine import ensure_schemas

    ensure_schemas(engine)
    Base.metadata.create_all(engine)
    _migrate_add_partition_key(engine)
