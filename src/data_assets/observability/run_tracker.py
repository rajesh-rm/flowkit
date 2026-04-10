"""Write run metrics to data_ops.run_history and update coverage_tracker."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select, update
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
    partition_key: str = "",
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
            partition_key=partition_key,
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
    partition_key: str = "",
) -> None:
    """Upsert the coverage_tracker row for an asset after a successful run."""
    now = datetime.now(UTC)
    values: dict = {"asset_name": asset_name, "partition_key": partition_key, "updated_at": now}
    if forward_watermark is not None:
        values["forward_watermark"] = forward_watermark
    if backward_watermark is not None:
        values["backward_watermark"] = backward_watermark

    update_set = {k: v for k, v in values.items() if k not in {"asset_name", "partition_key"}}

    dialect_name = engine.dialect.name

    with Session(engine) as session:
        if dialect_name == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        else:
            from sqlalchemy.dialects.mysql import insert

        stmt = insert(CoverageTracker).values(**values)

        if dialect_name == "postgresql":
            stmt = stmt.on_conflict_do_update(
                index_elements=["asset_name", "partition_key"],
                set_={k: stmt.excluded[k] for k in update_set},
            )
        else:
            stmt = stmt.on_duplicate_key_update(
                **{k: stmt.inserted[k] for k in update_set}
            )

        session.execute(stmt)
        session.commit()


def register_asset_metadata(engine: Engine, assets: dict[str, type]) -> None:
    """Upsert all registered assets into data_ops.asset_registry.

    Called once during initialization so that source_name, asset_type,
    target_schema, target_table, and load_strategy are queryable for
    ops dashboards — even before the first run completes.
    """
    if not assets:
        return

    now = datetime.now(UTC)
    rows = []
    for name, cls in assets.items():
        asset = cls()
        source = asset.source_name or None  # normalize "" to None
        desc = asset.description or None  # normalize "" to None
        rows.append({
            "asset_name": name,
            "description": desc,
            "asset_type": asset.asset_type,
            "source_name": source,
            "target_schema": asset.target_schema,
            "target_table": asset.target_table,
            "load_strategy": asset.load_strategy.value,
            "registered_at": now,
        })

    # Batch upsert — single INSERT for all assets
    exclude = {"asset_name", "registered_at"}
    update_set = {k: rows[0][k] for k in rows[0] if k not in exclude}

    dialect_name = engine.dialect.name
    if dialect_name == "postgresql":
        from sqlalchemy.dialects.postgresql import insert
    else:
        from sqlalchemy.dialects.mysql import insert

    stmt = insert(AssetRegistry).values(rows)

    if dialect_name == "postgresql":
        stmt = stmt.on_conflict_do_update(
            index_elements=["asset_name"],
            set_={k: stmt.excluded[k] for k in update_set},
        )
    else:
        stmt = stmt.on_duplicate_key_update(
            **{k: stmt.inserted[k] for k in update_set}
        )

    with Session(engine) as session:
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


def get_coverage(
    engine: Engine, asset_name: str, partition_key: str = "",
) -> CoverageTracker | None:
    """Read the coverage_tracker row for an asset."""
    with Session(engine) as session:
        return session.execute(
            select(CoverageTracker)
            .where(CoverageTracker.asset_name == asset_name)
            .where(CoverageTracker.partition_key == partition_key)
        ).scalar_one_or_none()
