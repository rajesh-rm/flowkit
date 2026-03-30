"""Checkpoint and run lock CRUD operations against data_ops tables."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

from sqlalchemy import delete, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_assets.db.models import Checkpoint, RunLock

logger = logging.getLogger(__name__)

STALE_LOCK_HOURS = 6


class LockError(Exception):
    """Raised when a run lock cannot be acquired."""


def acquire_lock(
    engine: Engine,
    asset_name: str,
    run_id: uuid.UUID,
    stale_threshold_hours: int = STALE_LOCK_HOURS,
) -> None:
    """Acquire a run lock for the given asset.

    If a stale lock exists (older than threshold), it is overridden with a warning.
    Raises LockError if a non-stale lock is held by another run.
    """
    worker_id = os.environ.get("AIRFLOW__CORE__HOSTNAME", "local")
    now = datetime.now(timezone.utc)

    with Session(engine) as session:
        existing = session.execute(
            select(RunLock).where(RunLock.asset_name == asset_name)
        ).scalar_one_or_none()

        if existing is not None:
            age = now - existing.locked_at.replace(tzinfo=timezone.utc)
            if age > timedelta(hours=stale_threshold_hours):
                logger.warning(
                    "Stale lock detected for '%s' (locked %s ago by %s, run %s). "
                    "Overriding.",
                    asset_name,
                    age,
                    existing.locked_by,
                    existing.run_id,
                )
                session.delete(existing)
                session.flush()
            else:
                raise LockError(
                    f"Asset '{asset_name}' is locked by run {existing.run_id} "
                    f"(locked {age} ago by {existing.locked_by})"
                )

        lock = RunLock(
            asset_name=asset_name,
            run_id=run_id,
            locked_at=now,
            locked_by=worker_id,
        )
        session.add(lock)
        session.commit()
    logger.info("Acquired run lock for '%s' (run %s)", asset_name, run_id)


def release_lock(engine: Engine, asset_name: str) -> None:
    """Release the run lock for the given asset."""
    with Session(engine) as session:
        session.execute(delete(RunLock).where(RunLock.asset_name == asset_name))
        session.commit()
    logger.info("Released run lock for '%s'", asset_name)


# --- Checkpoint operations ---


def get_checkpoints(
    engine: Engine, asset_name: str, run_id: uuid.UUID | None = None
) -> list[Checkpoint]:
    """Read all checkpoint rows for an asset (optionally filtered by run_id)."""
    with Session(engine) as session:
        stmt = select(Checkpoint).where(Checkpoint.asset_name == asset_name)
        if run_id is not None:
            stmt = stmt.where(Checkpoint.run_id == run_id)
        return list(session.execute(stmt).scalars().all())


def get_latest_checkpoint(
    engine: Engine, asset_name: str
) -> Checkpoint | None:
    """Get the most recent checkpoint for an asset (any run)."""
    with Session(engine) as session:
        stmt = (
            select(Checkpoint)
            .where(Checkpoint.asset_name == asset_name)
            .order_by(Checkpoint.updated_at.desc())
            .limit(1)
        )
        return session.execute(stmt).scalar_one_or_none()


def save_checkpoint(
    engine: Engine,
    run_id: uuid.UUID,
    asset_name: str,
    worker_id: str,
    checkpoint_type: str,
    checkpoint_value: dict,
    rows_so_far: int,
    status: str = "in_progress",
) -> None:
    """Insert or update a checkpoint row for a specific worker."""
    now = datetime.now(timezone.utc)
    with Session(engine) as session:
        existing = session.execute(
            select(Checkpoint).where(
                Checkpoint.run_id == run_id,
                Checkpoint.asset_name == asset_name,
                Checkpoint.worker_id == worker_id,
            )
        ).scalar_one_or_none()

        if existing:
            existing.checkpoint_value = checkpoint_value
            existing.rows_so_far = rows_so_far
            existing.status = status
            existing.updated_at = now
        else:
            cp = Checkpoint(
                run_id=run_id,
                asset_name=asset_name,
                worker_id=worker_id,
                checkpoint_type=checkpoint_type,
                checkpoint_value=checkpoint_value,
                rows_so_far=rows_so_far,
                status=status,
                updated_at=now,
            )
            session.add(cp)
        session.commit()


def clear_checkpoints(engine: Engine, asset_name: str) -> None:
    """Delete all checkpoint rows for the given asset."""
    with Session(engine) as session:
        session.execute(
            delete(Checkpoint).where(Checkpoint.asset_name == asset_name)
        )
        session.commit()
    logger.debug("Cleared checkpoints for '%s'", asset_name)
