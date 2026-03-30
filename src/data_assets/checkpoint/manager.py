"""Checkpoint, run lock, and stale-run takeover against data_ops tables."""

from __future__ import annotations

import logging
import os
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_assets.db.models import Checkpoint, RunLock

logger = logging.getLogger(__name__)


class LockError(Exception):
    """Raised when a run lock cannot be acquired."""


# ---------------------------------------------------------------------------
# Lock acquisition with stale-run takeover
# ---------------------------------------------------------------------------


def acquire_or_takeover(
    engine: Engine,
    asset_name: str,
    run_id: uuid.UUID,
    temp_table: str,
    stale_heartbeat_minutes: int = 20,
    max_run_hours: int = 5,
) -> tuple[str | None, uuid.UUID | None]:
    """Acquire lock for an asset, taking over abandoned runs if found.

    Staleness detection (either triggers takeover):
      - heartbeat_at older than stale_heartbeat_minutes (worker stopped making progress)
      - locked_at older than max_run_hours (run exceeded maximum wall time)

    Args:
        temp_table: Temp table name for this new run (used only on fresh start).
        stale_heartbeat_minutes: Minutes without a checkpoint update before
            the run is considered abandoned.
        max_run_hours: Maximum hours a run can be active before forced takeover.

    Returns:
        (inherited_temp_table, abandoned_run_id):
        - On fresh start: (None, None)
        - On takeover: (old temp table name, old run_id)

    Raises:
        LockError: if a non-stale lock is held by another worker.
    """
    worker_id = os.environ.get("AIRFLOW__CORE__HOSTNAME", "local")
    now = datetime.now(UTC)

    with Session(engine) as session:
        existing = session.execute(
            select(RunLock).where(RunLock.asset_name == asset_name)
        ).scalar_one_or_none()

        inherited_temp: str | None = None
        abandoned_run_id: uuid.UUID | None = None

        if existing is not None:
            heartbeat = (existing.heartbeat_at or existing.locked_at).replace(
                tzinfo=UTC
            )
            lock_start = existing.locked_at.replace(tzinfo=UTC)

            heartbeat_age = now - heartbeat
            run_age = now - lock_start

            heartbeat_stale = heartbeat_age > timedelta(
                minutes=stale_heartbeat_minutes
            )
            run_exceeded = run_age > timedelta(hours=max_run_hours)

            if heartbeat_stale or run_exceeded:
                reason = (
                    f"no heartbeat for {heartbeat_age}"
                    if heartbeat_stale
                    else f"exceeded {max_run_hours}h max run time"
                )
                logger.warning(
                    "Abandoned run detected for '%s' (%s, run %s by %s). "
                    "Taking over.",
                    asset_name,
                    reason,
                    existing.run_id,
                    existing.locked_by,
                )
                inherited_temp = existing.temp_table
                abandoned_run_id = existing.run_id
                session.delete(existing)
                session.flush()
            else:
                raise LockError(
                    f"Asset '{asset_name}' is locked by run {existing.run_id} "
                    f"(last heartbeat {heartbeat_age} ago, locked by "
                    f"{existing.locked_by})"
                )

        # Create new lock — on takeover, keep the inherited temp table
        lock = RunLock(
            asset_name=asset_name,
            run_id=run_id,
            locked_at=now,
            locked_by=worker_id,
            temp_table=inherited_temp or temp_table,
            heartbeat_at=now,
        )
        session.add(lock)
        session.commit()

    if inherited_temp:
        logger.info(
            "Took over run for '%s' — inheriting temp table '%s'",
            asset_name,
            inherited_temp,
        )
    else:
        logger.info("Acquired lock for '%s' (run %s)", asset_name, run_id)

    return inherited_temp, abandoned_run_id


def update_lock_temp_table(
    engine: Engine, asset_name: str, temp_table: str
) -> None:
    """Update the temp table name on the lock (e.g., when inherited table is gone)."""
    with Session(engine) as session:
        session.execute(
            update(RunLock)
            .where(RunLock.asset_name == asset_name)
            .values(temp_table=temp_table)
        )
        session.commit()


def release_lock(engine: Engine, asset_name: str) -> None:
    """Release the run lock for the given asset."""
    with Session(engine) as session:
        session.execute(delete(RunLock).where(RunLock.asset_name == asset_name))
        session.commit()
    logger.info("Released run lock for '%s'", asset_name)


# ---------------------------------------------------------------------------
# Checkpoint operations
# ---------------------------------------------------------------------------


def get_checkpoints(
    engine: Engine, asset_name: str, run_id: uuid.UUID | None = None
) -> list[Checkpoint]:
    """Read all checkpoint rows for an asset, ordered by update time.

    Returns checkpoints ordered by updated_at ASC so that when building
    a dict keyed by worker_id, the latest checkpoint per worker wins.
    """
    with Session(engine) as session:
        stmt = (
            select(Checkpoint)
            .where(Checkpoint.asset_name == asset_name)
            .order_by(Checkpoint.updated_at.asc())
        )
        if run_id is not None:
            stmt = stmt.where(Checkpoint.run_id == run_id)
        return list(session.execute(stmt).scalars().all())


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
    """Insert or update a checkpoint row for a specific worker.

    Also refreshes the heartbeat on the run lock so stale-run detection
    knows the run is still making progress.
    """
    now = datetime.now(UTC)
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

        # Refresh heartbeat on the lock
        session.execute(
            update(RunLock)
            .where(RunLock.asset_name == asset_name)
            .values(heartbeat_at=now)
        )

        session.commit()


def clear_checkpoints(engine: Engine, asset_name: str) -> None:
    """Delete all checkpoint rows for the given asset."""
    with Session(engine) as session:
        session.execute(
            delete(Checkpoint).where(Checkpoint.asset_name == asset_name)
        )
        session.commit()
    logger.debug("Cleared checkpoints for '%s'", asset_name)
