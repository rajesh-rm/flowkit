"""Parallel and sequential extraction with checkpoint-based resumption.

Three public functions (called by runner.py):
- extract_sequential()     — one thread, paginate via build_request loop
- extract_page_parallel()  — discover total pages, fan out across threads
- extract_entity_parallel() — fan out parent entity keys across threads

All share _fetch_pages() for the core request→parse→write→checkpoint loop.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from sqlalchemy.engine import Engine

from data_assets.checkpoint.manager import save_checkpoint
from data_assets.core.api_asset import APIAsset
from data_assets.core.enums import CheckpointType
from data_assets.core.run_context import RunContext
from data_assets.core.types import RequestSpec, SkippedRequestError
from data_assets.extract.api_client import APIClient
from data_assets.load.loader import write_to_temp

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _next_checkpoint(
    current: dict | None, state: "PaginationState", page_size: int
) -> dict:
    """Build next-page checkpoint from pagination state.

    Auto-increments page/offset when the asset's parse_response doesn't
    provide explicit values (e.g., GitHub returns next_page=None).
    """
    prev = current or {}
    return {
        "cursor": state.cursor,
        "next_offset": (
            state.next_offset
            if state.next_offset is not None
            else prev.get("next_offset", 0) + page_size
        ),
        "next_page": (
            state.next_page
            if state.next_page is not None
            else prev.get("next_page", 1) + 1
        ),
    }


# ---------------------------------------------------------------------------
# Shared fetch loop — used by all three modes
# ---------------------------------------------------------------------------

def _fetch_pages(
    asset: APIAsset,
    client: APIClient,
    engine: Engine,
    temp_table: str,
    context: RunContext,
    worker_id: str,
    request_builder: Callable[[dict | None], RequestSpec],
    initial_checkpoint: dict | None = None,
    on_page_complete: Callable[[dict, int], None] | None = None,
    log_interval_seconds: float | None = None,
    max_pages: int = 10_000,
    entity_key: Any = None,
) -> int:
    """Core extraction loop: request → parse → write → checkpoint → repeat.

    Args:
        asset: The APIAsset instance.
        client: Shared APIClient (thread-safe).
        engine: SQLAlchemy engine.
        temp_table: Temp table name in temp_store schema.
        context: Current RunContext.
        worker_id: Checkpoint worker identifier.
        request_builder: Callable(checkpoint) → RequestSpec.
            Each mode passes a different builder:
            - sequential: asset.build_request(context, checkpoint)
            - page-parallel: asset.build_request(context, {"page": N})
            - entity-parallel: asset.build_entity_request(key, context, checkpoint)
        initial_checkpoint: Saved pagination state to resume from, or None.
        on_page_complete: Optional callback(page_checkpoint, cumulative_rows).
            Called after each page is written. If provided, _fetch_pages does
            NOT save checkpoints itself — the caller is responsible (used by
            entity-parallel to save unified entity + pagination state).
        log_interval_seconds: If set, log progress every N seconds (used by
            sequential mode where total pages is unknown).
        max_pages: Safety limit to prevent infinite pagination loops.
        entity_key: If provided and asset has entity_key_column set, this value
            is injected as a column into the DataFrame after parse_response.

    Returns:
        Number of rows written to temp table.
    """
    rows = 0
    cp = initial_checkpoint
    page_count = 0
    start_time = time.monotonic()
    last_log_time = start_time

    while True:
        if page_count >= max_pages:
            logger.warning(
                "Worker %s: reached max_pages limit (%d). Stopping extraction.",
                worker_id, max_pages,
            )
            break

        spec = request_builder(cp)
        try:
            data = client.request(spec)
        except SkippedRequestError:
            logger.warning("Skipped request for worker %s", worker_id)
            break

        df, state = asset.parse_response(data)
        if entity_key is not None and asset.entity_key_column and not df.empty:
            df[asset.entity_key_column] = str(entity_key)
        rows += write_to_temp(engine, temp_table, df)
        page_count += 1

        if not state.has_more:
            break

        # Time-based progress logging (sequential mode)
        if log_interval_seconds is not None and time.monotonic() - last_log_time >= log_interval_seconds:
            elapsed = time.monotonic() - start_time
            logger.info(
                "Progress: %d rows (%d pages, %.0fs elapsed)",
                rows, page_count, elapsed,
            )
            last_log_time = time.monotonic()

        # Watermark-based early stop (e.g., GitHub PRs sorted by updated desc)
        if asset.should_stop(df, context):
            logger.info("Worker %s: should_stop() triggered, ending extraction", worker_id)
            break

        cp = _next_checkpoint(cp, state, asset.pagination_config.page_size)

        if on_page_complete:
            on_page_complete(cp, rows)
        else:
            save_checkpoint(
                engine,
                run_id=context.run_id,
                asset_name=asset.name,
                worker_id=worker_id,
                checkpoint_type=CheckpointType.SEQUENTIAL,
                checkpoint_value=cp,
                rows_so_far=rows,
                status="in_progress",
                partition_key=context.partition_key,
            )

    return rows


# ---------------------------------------------------------------------------
# Shared worker resume helper
# ---------------------------------------------------------------------------

def _resume_info(
    checkpoints: dict[str, dict], worker_id: str
) -> tuple[bool, int, dict | None]:
    """Check if a worker should be skipped or resumed.

    Returns:
        (skip_entirely, rows_so_far, checkpoint_value_or_None)
    """
    cp = checkpoints.get(worker_id)
    if not cp:
        return False, 0, None
    if cp.get("status") == "completed":
        return True, cp.get("rows_so_far", 0), None
    return False, cp.get("rows_so_far", 0), cp.get("checkpoint_value")


# ---------------------------------------------------------------------------
# Shared thread pool executor
# ---------------------------------------------------------------------------

def _run_workers(
    work_units: list[tuple[str, Any]],
    worker_fn: Callable[[str, Any], int],
    max_workers: int,
) -> int:
    """Submit work units to a thread pool and collect results.

    Args:
        work_units: List of (worker_id, work_data) tuples.
        worker_fn: Callable(worker_id, work_data) → rows.
        max_workers: Max threads (capped at actual work unit count).

    Returns:
        Total rows across all workers.
    """
    pool_size = min(max_workers, len(work_units))
    total_rows = 0

    with ThreadPoolExecutor(max_workers=pool_size) as pool:
        futures = {
            pool.submit(worker_fn, wid, wdata): wid
            for wid, wdata in work_units
        }
        for future in as_completed(futures):
            wid = futures[future]
            try:
                total_rows += future.result()
            except Exception:
                logger.exception("Worker %s failed", wid)
                pool.shutdown(wait=True, cancel_futures=True)
                raise

    return total_rows


# ---------------------------------------------------------------------------
# Public: Sequential extraction
# ---------------------------------------------------------------------------

def extract_sequential(
    asset: APIAsset,
    client: APIClient,
    engine: Engine,
    temp_table: str,
    context: RunContext,
    checkpoint: dict | None = None,
) -> int:
    """Sequential extraction with pagination and checkpoint support.

    Calls asset.build_request() each iteration, giving the asset full
    control over URL and params (supports multi-endpoint assets like
    GitHubRepos that iterate through multiple orgs).
    """
    rows_so_far = checkpoint.get("rows_so_far", 0) if checkpoint else 0
    cp = checkpoint.get("checkpoint_value") if checkpoint else None

    rows = _fetch_pages(
        asset, client, engine, temp_table, context,
        worker_id="main",
        request_builder=lambda c: asset.build_request(context, checkpoint=c),
        initial_checkpoint=cp,
        log_interval_seconds=30.0,
    )
    return rows_so_far + rows


# ---------------------------------------------------------------------------
# Public: Page-parallel extraction
# ---------------------------------------------------------------------------

def _discover_total_pages(
    asset: APIAsset, client: APIClient, engine: Engine,
    temp_table: str, context: RunContext,
) -> tuple[int, int | None, int | None]:
    """Fetch page 1 and derive total_pages.

    Returns (rows_written, total_pages, total_records).
    """
    first_spec = asset.build_request(context, checkpoint=None)
    first_data = client.request(first_spec)
    first_df, first_state = asset.parse_response(first_data)
    rows = write_to_temp(engine, temp_table, first_df)

    total_pages = first_state.total_pages
    if total_pages is None and first_state.total_records is not None:
        total_pages = math.ceil(
            first_state.total_records / asset.pagination_config.page_size
        )
    return rows, total_pages, first_state.total_records


def extract_page_parallel(
    asset: APIAsset,
    client: APIClient,
    engine: Engine,
    temp_table: str,
    context: RunContext,
    existing_checkpoints: dict[str, dict] | None = None,
) -> int:
    """Page-parallel: discover total pages from first request, fan out the rest.

    Step 1: Fetch page 1, read total_pages from response.
    Step 2: Partition pages 2..N across max_workers threads.
    Step 3: Each worker fetches its assigned pages sequentially.
    """
    existing_checkpoints = existing_checkpoints or {}

    rows_total, total_pages, total_records = _discover_total_pages(
        asset, client, engine, temp_table, context
    )
    if not total_pages or total_pages <= 1:
        return rows_total

    pool_size = min(asset.max_workers, total_pages - 1)
    logger.info(
        "Discovery: %d pages (~%s records), distributing to %d workers",
        total_pages,
        str(total_records) if total_records else "unknown",
        pool_size,
    )

    # Partition remaining pages
    remaining = list(range(2, total_pages + 1))
    chunk_size = max(1, math.ceil(len(remaining) / asset.max_workers))
    partitions = [
        remaining[i : i + chunk_size]
        for i in range(0, len(remaining), chunk_size)
    ]

    def page_worker(worker_id: str, pages: list[int]) -> int:
        skip, prior_rows, cp_value = _resume_info(existing_checkpoints, worker_id)
        if skip:
            logger.debug("Worker %s already completed, skipping", worker_id)
            return prior_rows

        # Resume: skip pages already fetched
        start_page = pages[0]
        if cp_value:
            last_page = cp_value.get("last_page", 0)
            start_page = last_page + 1
            logger.info("Worker %s resuming from page %d", worker_id, start_page)

        worker_rows = prior_rows
        pages_done = 0
        total_worker_pages = sum(1 for p in pages if p >= start_page)
        for page_num in pages:
            if page_num < start_page:
                continue

            spec = asset.build_request(context, checkpoint={"next_page": page_num})
            data = client.request(spec)
            df, _ = asset.parse_response(data)
            worker_rows += write_to_temp(engine, temp_table, df)
            pages_done += 1

            if pages_done % 10 == 0 or pages_done == total_worker_pages:
                logger.info(
                    "Worker %s: page %d/%d (%d rows)",
                    worker_id, pages_done, total_worker_pages, worker_rows,
                )

            save_checkpoint(
                engine,
                run_id=context.run_id,
                asset_name=asset.name,
                worker_id=worker_id,
                checkpoint_type=CheckpointType.PAGE_PARALLEL,
                checkpoint_value={"last_page": page_num},
                rows_so_far=worker_rows,
                status="in_progress",
                partition_key=context.partition_key,
            )

        save_checkpoint(
            engine,
            run_id=context.run_id,
            asset_name=asset.name,
            worker_id=worker_id,
            checkpoint_type=CheckpointType.PAGE_PARALLEL,
            checkpoint_value={"last_page": pages[-1]},
            rows_so_far=worker_rows,
            status="completed",
            partition_key=context.partition_key,
        )
        return worker_rows

    work_units = [
        (f"pages_{p[0]}_{p[-1]}", p) for p in partitions
    ]
    rows_total += _run_workers(work_units, page_worker, asset.max_workers)
    return rows_total


# ---------------------------------------------------------------------------
# Public: Entity-parallel extraction
# ---------------------------------------------------------------------------

def _parse_entity_resume(
    cp_value: dict | None,
) -> tuple[set[str], str | None, dict | None]:
    """Parse entity-parallel resume state from checkpoint value."""
    if not cp_value:
        return set(), None, None
    return (
        set(cp_value.get("completed_entities", [])),
        cp_value.get("current_entity"),
        cp_value.get("pagination_state"),
    )


def extract_entity_parallel(
    asset: APIAsset,
    client: APIClient,
    engine: Engine,
    temp_table: str,
    context: RunContext,
    entity_keys: list[Any],
    existing_checkpoints: dict[str, dict] | None = None,
) -> int:
    """Entity-parallel: fan out parent entity keys across threads.

    Each worker iterates its assigned entities, paginating fully within
    each one. If an entity returns 404, it's skipped (not fatal).
    """
    existing_checkpoints = existing_checkpoints or {}

    chunk_size = max(1, math.ceil(len(entity_keys) / asset.max_workers))
    partitions = [
        entity_keys[i : i + chunk_size]
        for i in range(0, len(entity_keys), chunk_size)
    ]

    def entity_worker(worker_id: str, entities: list[Any]) -> int:
        skip, prior_rows, cp_value = _resume_info(existing_checkpoints, worker_id)
        if skip:
            logger.debug("Worker %s already completed, skipping", worker_id)
            return prior_rows

        completed, resume_entity, resume_pagination = _parse_entity_resume(cp_value)

        worker_rows = prior_rows
        total_entities = len(entities)
        entities_done = 0
        log_interval = max(1, total_entities // 5)

        for entity_key in entities:
            entity_str = str(entity_key)
            if entity_str in completed:
                entities_done += 1
                continue

            # Resume pagination within this entity if applicable
            page_cp = resume_pagination if entity_str == resume_entity else None
            if page_cp:
                resume_entity = None  # Only apply once

            # Callback saves unified checkpoint: entity progress + page position.
            # This ensures that if the worker dies mid-entity, the next worker
            # knows both which entities are done AND where within the current
            # entity to resume.
            def on_entity_page(page_state, page_rows, _ek=entity_str, _wr=worker_rows):
                save_checkpoint(
                    engine,
                    run_id=context.run_id,
                    asset_name=asset.name,
                    worker_id=worker_id,
                    checkpoint_type=CheckpointType.ENTITY_PARALLEL,
                    checkpoint_value={
                        "completed_entities": list(completed),
                        "current_entity": _ek,
                        "pagination_state": page_state,
                    },
                    rows_so_far=_wr + page_rows,
                    status="in_progress",
                    partition_key=context.partition_key,
                )

            # Fetch all pages for this entity
            entity_rows = _fetch_pages(
                asset, client, engine, temp_table, context,
                worker_id=worker_id,
                request_builder=lambda c, ek=entity_key: (
                    asset.build_entity_request(ek, context, checkpoint=c)
                ),
                initial_checkpoint=page_cp,
                on_page_complete=on_entity_page,
                entity_key=entity_key,
            )
            worker_rows += entity_rows

            # Mark entity complete AFTER all its pages succeeded
            completed.add(entity_str)
            entities_done += 1

            if entities_done % log_interval == 0 or entities_done == total_entities:
                logger.info(
                    "Worker %s: %d/%d entities (%d rows)",
                    worker_id, entities_done, total_entities, worker_rows,
                )

            save_checkpoint(
                engine,
                run_id=context.run_id,
                asset_name=asset.name,
                worker_id=worker_id,
                checkpoint_type=CheckpointType.ENTITY_PARALLEL,
                checkpoint_value={"completed_entities": list(completed)},
                rows_so_far=worker_rows,
                status="in_progress",
                partition_key=context.partition_key,
            )

        save_checkpoint(
            engine,
            run_id=context.run_id,
            asset_name=asset.name,
            worker_id=worker_id,
            checkpoint_type=CheckpointType.ENTITY_PARALLEL,
            checkpoint_value={"completed_entities": list(completed)},
            rows_so_far=worker_rows,
            status="completed",
            partition_key=context.partition_key,
        )
        return worker_rows

    work_units = [
        (f"entities_{idx}", ents) for idx, ents in enumerate(partitions)
    ]
    return _run_workers(work_units, entity_worker, asset.max_workers)
