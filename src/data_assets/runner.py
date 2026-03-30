"""Main orchestrator: run_asset() implements the full run lifecycle.

See architecture doc Section 9 for the complete lifecycle specification.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import UTC, datetime

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_assets.checkpoint.manager import (
    acquire_lock,
    clear_checkpoints,
    get_checkpoints,
    release_lock,
)
from data_assets.core.api_asset import APIAsset
from data_assets.core.asset import Asset
from data_assets.core.enums import ParallelMode, RunMode
from data_assets.core.registry import discover, get
from data_assets.core.run_context import RunContext
from data_assets.core.transform_asset import TransformAsset
from data_assets.db.engine import get_engine
from data_assets.db.models import CoverageTracker, RunHistory, create_all_tables
from data_assets.extract.api_client import APIClient
from data_assets.extract.parallel import (
    extract_entity_parallel,
    extract_page_parallel,
    extract_sequential,
)
from data_assets.extract.rate_limiter import RateLimiter
from data_assets.load.loader import (
    create_temp_table,
    drop_table,
    drop_temp_table,
    promote,
    read_temp_table,
    temp_table_exists,
    temp_table_name,
    write_to_temp,
)
from data_assets.observability.logging import setup_logging
from data_assets.observability.run_tracker import (
    get_coverage,
    record_run_failure,
    record_run_start,
    record_run_success,
    update_coverage,
    update_last_success,
)
from data_assets.transform.db_transform import execute_transform

logger = logging.getLogger(__name__)

_initialized = False


def _ensure_initialized(engine: Engine) -> None:
    """One-time initialization: create schemas/tables, discover assets."""
    global _initialized
    if _initialized:
        return
    create_all_tables(engine)
    discover()
    _initialized = True


def run_asset(
    asset_name: str,
    run_mode: str = "full",
    **overrides,
) -> dict:
    """Execute a complete ETL run for the named asset.

    Args:
        asset_name: Name matching a registered asset class.
        run_mode: One of "full", "forward", "backfill", "transform".
        **overrides: Runtime overrides including:
            - rate_limit_per_second, max_workers, request_timeout, max_retries
            - start_date, end_date (override computed date window)
            - airflow_run_id (links to Airflow)
            - dry_run=True (extract + validate, skip promotion)

    Returns:
        Dict with run metrics.
    """
    setup_logging()
    start_time = time.monotonic()
    run_id = uuid.uuid4()
    mode = RunMode(run_mode)
    dry_run = overrides.pop("dry_run", False)

    engine = get_engine()
    _ensure_initialized(engine)

    asset_cls = get(asset_name)
    asset = asset_cls()

    logger.info(
        "Starting run: asset=%s mode=%s run_id=%s dry_run=%s",
        asset_name, mode.value, run_id, dry_run,
    )

    acquire_lock(engine, asset_name, run_id)

    rows_extracted = 0
    rows_loaded = 0
    client_stats: dict = {}

    try:
        coverage = get_coverage(engine, asset_name)
        start_date, end_date = _compute_date_window(mode, coverage, overrides)

        existing_cps = get_checkpoints(engine, asset_name)
        existing_cp_map: dict[str, dict] = {}
        for cp in existing_cps:
            existing_cp_map[cp.worker_id] = {
                "checkpoint_value": cp.checkpoint_value,
                "rows_so_far": cp.rows_so_far,
                "status": cp.status,
            }

        context = RunContext(
            run_id=run_id, mode=mode, asset_name=asset_name,
            start_date=start_date, end_date=end_date, params=overrides,
        )

        record_run_start(
            engine, run_id=run_id, asset_name=asset_name, run_mode=mode.value,
            airflow_run_id=overrides.get("airflow_run_id"),
            metadata={"start_date": str(start_date), "end_date": str(end_date)},
        )

        # --- Phase 2: Extract ---
        extract_start = time.monotonic()

        temp_tbl = temp_table_name(asset_name, run_id)
        if not temp_table_exists(engine, temp_tbl):
            temp_tbl = create_temp_table(engine, asset_name, run_id, asset.columns)

        if isinstance(asset, APIAsset):
            rows_extracted, client_stats = _extract_api(
                asset, engine, temp_tbl, context, existing_cp_map, overrides
            )
        elif isinstance(asset, TransformAsset):
            query = asset.query(context)
            rows_extracted = execute_transform(engine, query, temp_tbl, context)
        else:
            raise TypeError(f"Unknown asset type: {type(asset)}")

        extract_seconds = round(time.monotonic() - extract_start, 2)

        # Row count anomaly warning
        _check_row_count_anomaly(engine, asset_name, rows_extracted)

        # --- Phase 3: Transform & Validate ---
        df = read_temp_table(engine, temp_tbl)

        has_custom_transform = type(asset).transform is not Asset.transform
        if has_custom_transform:
            df = asset.transform(df)
            if len(df) > 0:
                drop_table(engine, "temp_store", temp_tbl)
                temp_tbl = create_temp_table(engine, asset_name, run_id, asset.columns)
                write_to_temp(engine, temp_tbl, df)

        validation_result = asset.validate(df, context)
        if not validation_result.passed:
            raise ValueError(
                f"Validation failed for '{asset_name}': "
                + "; ".join(validation_result.failures)
            )

        # Collect non-blocking warnings
        warnings = asset.validate_warnings(df, context)
        if warnings:
            for w in warnings:
                logger.warning("Validation warning for '%s': %s", asset_name, w)

        # --- Phase 4: Promote (skip if dry_run) ---
        promote_start = time.monotonic()

        if dry_run:
            logger.info("Dry run — skipping promotion for '%s'", asset_name)
            rows_loaded = 0
        else:
            rows_loaded = promote(
                engine=engine, temp_table=temp_tbl,
                target_schema=asset.target_schema, target_table=asset.target_table,
                columns=asset.columns, primary_key=asset.primary_key,
                load_strategy=asset.load_strategy,
                schema_contract=asset.schema_contract,
            )

        promote_seconds = round(time.monotonic() - promote_start, 2)

        # --- Phase 5: Finalize ---
        if not dry_run:
            _update_watermarks(engine, asset, mode, start_date, end_date, df)
            update_last_success(engine, asset_name)

        # Build run metadata
        run_metadata = {
            "extraction_seconds": extract_seconds,
            "promotion_seconds": promote_seconds,
            "warnings": warnings,
            **client_stats,
        }

        record_run_success(engine, run_id, rows_extracted, rows_loaded,
                           metadata=run_metadata)
        clear_checkpoints(engine, asset_name)
        drop_temp_table(engine, temp_tbl)
        release_lock(engine, asset_name)

        duration = time.monotonic() - start_time
        status = "dry_run" if dry_run else "success"
        logger.info(
            "Run complete: asset=%s rows_extracted=%d rows_loaded=%d duration=%.1fs status=%s",
            asset_name, rows_extracted, rows_loaded, duration, status,
        )

        return {
            "run_id": str(run_id),
            "asset_name": asset_name,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
            "duration_seconds": round(duration, 2),
            "status": status,
            "metadata": run_metadata,
        }

    except Exception as exc:
        error_msg = str(exc)
        logger.exception("Run failed: asset=%s error=%s", asset_name, error_msg)
        record_run_failure(engine, run_id, error_msg)
        release_lock(engine, asset_name)
        raise


def _extract_api(
    asset: APIAsset,
    engine: Engine,
    temp_tbl: str,
    context: RunContext,
    existing_cp_map: dict[str, dict],
    overrides: dict,
) -> tuple[int, dict]:
    """Handle API extraction. Returns (rows_extracted, client_stats)."""
    rate = overrides.get("rate_limit_per_second", asset.rate_limit_per_second)
    timeout = overrides.get("request_timeout", asset.request_timeout)
    retries = overrides.get("max_retries", asset.max_retries)

    token_mgr = asset.token_manager_class()
    rate_limiter = RateLimiter(rate)
    client = APIClient(
        token_mgr, rate_limiter, timeout=timeout, max_retries=retries,
        error_classifier=asset.classify_error,
    )

    try:
        if asset.parallel_mode == ParallelMode.PAGE_PARALLEL:
            rows = extract_page_parallel(
                asset, client, engine, temp_tbl, context, existing_cp_map
            )
        elif asset.parallel_mode == ParallelMode.ENTITY_PARALLEL:
            entity_keys = _load_entity_keys(engine, asset)
            rows = extract_entity_parallel(
                asset, client, engine, temp_tbl, context, entity_keys, existing_cp_map
            )
        else:
            main_cp = existing_cp_map.get("main")
            rows = extract_sequential(
                asset, client, engine, temp_tbl, context, main_cp
            )
        return rows, client.stats
    finally:
        client.close()


def _load_entity_keys(engine: Engine, asset: APIAsset) -> list:
    """Load parent entity primary keys for entity-parallel extraction."""
    from data_assets.core.registry import get as get_asset_cls

    parent_cls = get_asset_cls(asset.parent_asset_name)
    parent = parent_cls()
    pk_cols = ", ".join(f'"{c}"' for c in parent.primary_key)
    query = f'SELECT {pk_cols} FROM "{parent.target_schema}"."{parent.target_table}"'
    df = pd.read_sql(query, engine)
    if len(parent.primary_key) == 1:
        return df[parent.primary_key[0]].tolist()
    return df[parent.primary_key].to_dict("records")


def _compute_date_window(
    mode: RunMode, coverage: CoverageTracker | None, overrides: dict,
) -> tuple[datetime | None, datetime | None]:
    now = datetime.now(UTC)
    if "start_date" in overrides and "end_date" in overrides:
        return overrides["start_date"], overrides["end_date"]
    if mode == RunMode.FULL:
        return None, None
    if mode == RunMode.FORWARD:
        return (coverage.forward_watermark if coverage else None), now
    if mode == RunMode.BACKFILL:
        return None, (coverage.backward_watermark if coverage else now)
    return None, None


def _update_watermarks(
    engine: Engine, asset, mode: RunMode,
    start_date: datetime | None, end_date: datetime | None,
    df: pd.DataFrame,
) -> None:
    if not hasattr(asset, "date_column") or not asset.date_column:
        return
    if asset.date_column not in df.columns:
        return

    col = pd.to_datetime(df[asset.date_column], utc=True, errors="coerce").dropna()
    if col.empty:
        return

    max_date = col.max().to_pydatetime()
    min_date = col.min().to_pydatetime()

    if mode in (RunMode.FULL, RunMode.FORWARD):
        update_coverage(engine, asset.name, forward_watermark=max_date)
    if mode in (RunMode.FULL, RunMode.BACKFILL):
        update_coverage(engine, asset.name, backward_watermark=min_date)


def _check_row_count_anomaly(
    engine: Engine, asset_name: str, rows_extracted: int
) -> None:
    """Warn if row count is significantly below recent average."""
    try:
        with Session(engine) as session:
            result = session.execute(
                select(func.avg(RunHistory.rows_extracted))
                .where(RunHistory.asset_name == asset_name)
                .where(RunHistory.status == "success")
                .order_by(RunHistory.completed_at.desc())
                .limit(5)
            ).scalar()
            if result and rows_extracted < result * 0.5:
                logger.warning(
                    "Row count anomaly for '%s': got %d, recent average is %.0f",
                    asset_name, rows_extracted, result,
                )
    except Exception:
        pass  # Non-critical — don't fail the run for a warning query
