"""Write run metrics to data_ops.run_history and update coverage_tracker."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_assets.db.models import AssetRegistry, CoverageTracker, RunHistory


def record_run_start(
    engine: Engine,
    run_id: uuid.UUID,
    asset_name: str,
    run_mode: str,
    airflow_run_id: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Insert an in-progress run_history row at the start of a run."""
    with Session(engine) as session:
        row = RunHistory(
            run_id=run_id,
            asset_name=asset_name,
            run_mode=run_mode,
            status="running",
            started_at=datetime.now(UTC),
            metadata_=metadata or {},
            airflow_run_id=airflow_run_id,
        )
        session.add(row)
        session.commit()


def record_run_success(
    engine: Engine,
    run_id: uuid.UUID,
    rows_extracted: int,
    rows_loaded: int,
    metadata: dict | None = None,
) -> None:
    """Update the run_history row on successful completion."""
    now = datetime.now(UTC)
    values: dict = {
        "status": "success",
        "completed_at": now,
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
    }
    if metadata:
        values["metadata_"] = metadata
    with Session(engine) as session:
        session.execute(
            update(RunHistory).where(RunHistory.run_id == run_id).values(**values)
        )
        session.commit()


def record_run_failure(
    engine: Engine,
    run_id: uuid.UUID,
    error_message: str,
) -> None:
    """Update the run_history row on failure."""
    now = datetime.now(UTC)
    with Session(engine) as session:
        session.execute(
            update(RunHistory)
            .where(RunHistory.run_id == run_id)
            .values(
                status="failed",
                completed_at=now,
                error_message=error_message,
            )
        )
        session.commit()


def update_coverage(
    engine: Engine,
    asset_name: str,
    forward_watermark: datetime | None = None,
    backward_watermark: datetime | None = None,
) -> None:
    """Upsert the coverage_tracker row for an asset after a successful run."""
    now = datetime.now(UTC)
    values: dict = {"asset_name": asset_name, "updated_at": now}
    update_set: dict = {"updated_at": now}

    if forward_watermark is not None:
        values["forward_watermark"] = forward_watermark
        update_set["forward_watermark"] = forward_watermark
    if backward_watermark is not None:
        values["backward_watermark"] = backward_watermark
        update_set["backward_watermark"] = backward_watermark

    with Session(engine) as session:
        stmt = (
            pg_insert(CoverageTracker)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["asset_name"],
                set_=update_set,
            )
        )
        session.execute(stmt)
        session.commit()


def update_last_success(engine: Engine, asset_name: str) -> None:
    """Set last_success_at on the asset_registry row."""
    now = datetime.now(UTC)
    with Session(engine) as session:
        session.execute(
            update(AssetRegistry)
            .where(AssetRegistry.asset_name == asset_name)
            .values(last_success_at=now)
        )
        session.commit()


def get_coverage(engine: Engine, asset_name: str) -> CoverageTracker | None:
    """Read the coverage_tracker row for an asset."""
    from sqlalchemy import select

    with Session(engine) as session:
        return session.execute(
            select(CoverageTracker).where(CoverageTracker.asset_name == asset_name)
        ).scalar_one_or_none()
