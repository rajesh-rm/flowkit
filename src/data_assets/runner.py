"""Main orchestrator: run_asset() implements the full run lifecycle.

See docs/architecture.md for the complete lifecycle specification.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import UTC, datetime

import pandas as pd
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from data_assets.checkpoint.manager import (
    acquire_or_takeover,
    checkpoints_by_worker,
    clear_checkpoints,
    get_checkpoints,
    release_lock,
    update_lock_temp_table,
)
from data_assets.core.api_asset import APIAsset
from data_assets.core.asset import Asset
from data_assets.core.enums import ParallelMode, RunMode
from data_assets.core.identifiers import uuid7
from data_assets.core.registry import all_assets, discover, get
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
    TEMP_SCHEMA,
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
    register_asset_metadata,
    update_coverage,
    update_last_success,
)
from data_assets.transform.db_transform import execute_transform

logger = logging.getLogger(__name__)

# Warn if row count drops below this fraction of the recent 5-run average.
ROW_COUNT_ANOMALY_THRESHOLD = 0.5

_initialized = False
_init_lock = threading.Lock()


def _ensure_initialized(engine: Engine) -> None:
    """One-time initialization: create schemas/tables, discover assets, register metadata."""
    global _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:  # double-check after acquiring lock
            return
        create_all_tables(engine)
        discover()
        register_asset_metadata(engine, all_assets())
        _initialized = True


def run_asset(
    asset_name: str,
    run_mode: str = "full",
    secrets: dict[str, str] | None = None,
    **overrides,
) -> dict:
    """Execute a complete ETL run for the named asset.

    Args:
        asset_name: Name matching a registered asset class.
        run_mode: One of "full", "forward", "backfill", "transform".
        secrets: Credentials to inject as environment variables for this run.
            Keys are env var names, values are the secret strings. Injected
            before token managers initialize and cleaned up after the run.
            This is the primary way for Airflow DAGs to pass secrets from
            Connections, Variables, or secret backends.
        **overrides: Runtime overrides including:
            - rate_limit_per_second, max_workers, request_timeout, max_retries
            - start_date, end_date (override computed date window)
            - airflow_run_id (links to Airflow)
            - dry_run=True (extract + validate, skip promotion)

    Returns:
        Dict with run metrics.

    Example (Airflow DAG)::

        from airflow.hooks.base import BaseHook

        def _run_github(**kwargs):
            conn = BaseHook.get_connection("github_app")
            run_asset(
                "github_repos",
                run_mode="full",
                secrets={
                    "GITHUB_APP_ID": conn.login,
                    "GITHUB_PRIVATE_KEY": conn.password,
                    "GITHUB_INSTALLATION_ID": conn.extra_dejson["installation_id"],
                    "GITHUB_ORGS": conn.extra_dejson["orgs"],
                },
                airflow_run_id=kwargs["run_id"],
            )
    """
    setup_logging()
    start_time = time.monotonic()
    run_id = uuid7()
    mode = RunMode(run_mode)
    dry_run = overrides.pop("dry_run", False)

    # Inject secrets as env vars for this run — cleaned up in finally block
    _injected_secrets: list[str] = []
    if secrets:
        for key, value in secrets.items():
            os.environ[key] = value
            _injected_secrets.append(key)

    try:
        engine = get_engine()
        _ensure_initialized(engine)

        asset_cls = get(asset_name)
        asset = asset_cls()

        logger.info(
            "Starting run: asset=%s mode=%s run_id=%s dry_run=%s",
            asset_name, mode.value, run_id, dry_run,
        )

        # --- Phase 1: Acquire lock (or take over abandoned run) ---
        stale_minutes = asset.stale_heartbeat_minutes
        max_hours = asset.max_run_hours
        new_temp = temp_table_name(asset_name, run_id)

        inherited_temp, abandoned_run_id = acquire_or_takeover(
            engine, asset_name, run_id, new_temp,
            stale_heartbeat_minutes=stale_minutes,
            max_run_hours=max_hours,
        )

        rows_extracted = 0
        rows_loaded = 0
        client_stats: dict = {}

        try:
            # Mark abandoned run in history if we took over
            if abandoned_run_id:
                record_run_failure(engine, abandoned_run_id, "Abandoned — taken over by new worker")

            coverage = get_coverage(engine, asset_name)
            start_date, end_date = _compute_date_window(mode, coverage, overrides)

            existing_cp_map = checkpoints_by_worker(
                get_checkpoints(engine, asset_name)
            )

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

            # WHY: Reusing the inherited temp table preserves partial extraction
            # data from the abandoned run, avoiding re-fetching already-written pages.
            if inherited_temp and temp_table_exists(engine, inherited_temp):
                temp_tbl = inherited_temp
                logger.info(
                    "Reusing temp table '%s' from abandoned run %s",
                    temp_tbl, abandoned_run_id,
                )
            else:
                temp_tbl = create_temp_table(engine, asset_name, run_id, asset.columns)
                if inherited_temp:
                    # Inherited table was gone (e.g., Postgres crash); update lock
                    update_lock_temp_table(engine, asset_name, temp_tbl)

            has_custom_extract = type(asset).extract is not Asset.extract
            if has_custom_extract:
                rows_extracted = asset.extract(engine, temp_tbl, context)
            elif isinstance(asset, APIAsset):
                rows_extracted, client_stats = _extract_api(
                    asset, engine, temp_tbl, context, existing_cp_map, overrides
                )
            elif isinstance(asset, TransformAsset):
                _check_source_freshness(engine, asset)
                query = asset.query(context)
                rows_extracted = execute_transform(engine, query, temp_tbl, context)
            else:
                raise TypeError(
                    f"Asset '{asset_name}' has type {type(asset).__name__}, "
                    f"expected APIAsset or TransformAsset"
                )

            extract_seconds = round(time.monotonic() - extract_start, 2)

            # Row count anomaly warning
            _check_row_count_anomaly(engine, asset_name, rows_extracted)

            # --- Phase 3: Transform & Validate ---
            df = read_temp_table(engine, temp_tbl)

            has_custom_transform = type(asset).transform is not Asset.transform
            if has_custom_transform:
                rows_before = len(df)
                df = asset.transform(df)
                drop_table(engine, TEMP_SCHEMA, temp_tbl)
                temp_tbl = create_temp_table(engine, asset_name, run_id, asset.columns)
                write_to_temp(engine, temp_tbl, df)
                if len(df) != rows_before:
                    logger.info(
                        "Transform: %d rows → %d rows", rows_before, len(df),
                    )

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
                _update_watermarks(engine, asset, mode, df)
                update_last_success(engine, asset_name)

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
            try:
                record_run_failure(engine, run_id, error_msg)
            finally:
                if "temp_tbl" in locals():
                    try:
                        drop_temp_table(engine, temp_tbl)
                    except Exception:
                        logger.debug("Failed to drop temp table on error cleanup", exc_info=True)
                release_lock(engine, asset_name)
            raise
    finally:
        # Clean up injected secrets so they don't leak to other tasks
        # in the same worker process
        for key in _injected_secrets:
            os.environ.pop(key, None)


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

    if not asset.token_manager_class:
        raise ValueError(
            f"Asset '{asset.name}' has no token_manager_class set. "
            "All API assets must configure a TokenManager."
        )
    token_mgr = asset.token_manager_class()
    rate_limiter = RateLimiter(rate)
    client = APIClient(
        token_mgr, rate_limiter, timeout=timeout, max_retries=retries,
        error_classifier=asset.classify_error,
    )

    try:
        extract_start = time.monotonic()

        if asset.parallel_mode == ParallelMode.PAGE_PARALLEL:
            logger.info("Extracting %s (page-parallel, %d workers)", asset.name, asset.max_workers)
            rows = extract_page_parallel(
                asset, client, engine, temp_tbl, context, existing_cp_map
            )
        elif asset.parallel_mode == ParallelMode.ENTITY_PARALLEL:
            entity_keys = _load_entity_keys(engine, asset)
            entity_keys = asset.filter_entity_keys(entity_keys)
            logger.info(
                "Extracting %s (entity-parallel, %d entities, %d workers)",
                asset.name, len(entity_keys), asset.max_workers,
            )
            rows = extract_entity_parallel(
                asset, client, engine, temp_tbl, context, entity_keys, existing_cp_map
            )
        else:
            logger.info("Extracting %s (sequential)", asset.name)
            main_cp = existing_cp_map.get("main")
            rows = extract_sequential(
                asset, client, engine, temp_tbl, context, main_cp
            )

        duration = time.monotonic() - extract_start
        api_calls = client.stats.get("api_calls", 0)
        logger.info(
            "Extraction complete: %d rows in %.1fs (%d API calls)",
            rows, duration, api_calls,
        )
        return rows, client.stats
    finally:
        client.close()


def _load_entity_keys(engine: Engine, asset: APIAsset) -> list:
    """Load parent entity primary keys for entity-parallel extraction.

    Returns:
        list of scalar values (str/int) when parent has a single-column PK,
        list of dicts when parent has a composite PK. All current assets use
        single-column PKs, so callers receive list[str] in practice.
    """
    parent_cls = get(asset.parent_asset_name)
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
    if mode == RunMode.FULL or mode == RunMode.TRANSFORM:
        return None, None
    if mode == RunMode.FORWARD:
        return (coverage.forward_watermark if coverage else None), now
    if mode == RunMode.BACKFILL:
        return None, (coverage.backward_watermark if coverage else now)
    return None, None


def _update_watermarks(
    engine: Engine, asset, mode: RunMode, df: pd.DataFrame,
) -> None:
    if not hasattr(asset, "date_column") or not asset.date_column:
        return
    if asset.date_column not in df.columns:
        return

    raw_col = df[asset.date_column]
    col = pd.to_datetime(raw_col, utc=True, errors="coerce")
    bad_count = int(col.isna().sum() - raw_col.isna().sum())
    if bad_count > 0:
        logger.warning(
            "Asset '%s': %d of %d values in '%s' could not be parsed as dates",
            asset.name, bad_count, len(raw_col), asset.date_column,
        )
    col = col.dropna()
    if col.empty:
        return

    max_date = col.max().to_pydatetime()
    min_date = col.min().to_pydatetime()

    if mode in (RunMode.FULL, RunMode.FORWARD):
        update_coverage(engine, asset.name, forward_watermark=max_date)
    if mode in (RunMode.FULL, RunMode.BACKFILL):
        update_coverage(engine, asset.name, backward_watermark=min_date)


def _check_source_freshness(
    engine: Engine, asset, max_stale_hours: int = 24,
) -> None:
    """Warn if a transform's source tables haven't been refreshed recently."""
    source_tables = getattr(asset, "source_tables", [])
    if not source_tables:
        return

    try:
        from data_assets.db.models import AssetRegistry

        now = datetime.now(UTC)
        with Session(engine) as session:
            for table in source_tables:
                row = session.execute(
                    select(AssetRegistry.last_success_at)
                    .where(AssetRegistry.target_table == table)
                ).scalar()
                if row is None:
                    logger.warning(
                        "Transform '%s': source table '%s' has never been loaded.",
                        asset.name, table,
                    )
                elif (now - row).total_seconds() > max_stale_hours * 3600:
                    hours_ago = (now - row).total_seconds() / 3600
                    logger.warning(
                        "Transform '%s': source table '%s' is %.0f hours stale "
                        "(threshold: %d hours).",
                        asset.name, table, hours_ago, max_stale_hours,
                    )
    except Exception:
        logger.warning("Source freshness check failed for '%s'", asset.name, exc_info=True)


def _check_row_count_anomaly(
    engine: Engine, asset_name: str, rows_extracted: int
) -> None:
    """Warn if row count is significantly below recent average."""
    try:
        with Session(engine) as session:
            recent = (
                select(RunHistory.rows_extracted)
                .where(RunHistory.asset_name == asset_name)
                .where(RunHistory.status == "success")
                .order_by(RunHistory.completed_at.desc())
                .limit(5)
            ).subquery()
            result = session.execute(
                select(func.avg(recent.c.rows_extracted))
            ).scalar()
            if result and rows_extracted < result * ROW_COUNT_ANOMALY_THRESHOLD:
                logger.warning(
                    "Row count anomaly for '%s': got %d, recent average is %.0f",
                    asset_name, rows_extracted, result,
                )
    except Exception:
        logger.warning("Row count anomaly check failed for '%s'", asset_name, exc_info=True)
