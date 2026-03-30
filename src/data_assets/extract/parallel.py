"""Thread-pool based parallel extraction for page-parallel and entity-parallel modes."""

from __future__ import annotations

import logging
import math
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from data_assets.checkpoint.manager import save_checkpoint
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationState, RequestSpec
from data_assets.extract.api_client import APIClient
from data_assets.extract.pagination import next_request_params
from data_assets.load.temp_table import write_to_temp

logger = logging.getLogger(__name__)


def extract_page_parallel(
    asset: Any,  # APIAsset
    client: APIClient,
    engine: Engine,
    temp_table: str,
    context: RunContext,
    existing_checkpoints: dict[str, dict] | None = None,
) -> int:
    """Page-parallel extraction: discover total pages, then fan out.

    Returns total rows extracted.
    """
    existing_checkpoints = existing_checkpoints or {}

    # Step 1: Discovery call (page 1)
    first_spec = asset.build_request(context, checkpoint=None)
    first_data = client.request(first_spec)
    first_df, first_state = asset.parse_response(first_data)

    rows_total = write_to_temp(engine, temp_table, first_df)

    total_pages = first_state.total_pages
    if total_pages is None and first_state.total_records is not None:
        total_pages = math.ceil(first_state.total_records / asset.pagination_config.page_size)

    if not total_pages or total_pages <= 1:
        return rows_total

    # Step 2: Partition remaining pages across workers
    remaining = list(range(2, total_pages + 1))
    chunk_size = max(1, math.ceil(len(remaining) / asset.max_workers))
    partitions = [
        remaining[i : i + chunk_size] for i in range(0, len(remaining), chunk_size)
    ]

    # Step 3: Fan out
    def worker(pages: list[int], worker_id: str) -> int:
        cp_info = existing_checkpoints.get(worker_id)
        if cp_info and cp_info.get("status") == "completed":
            logger.info("Worker %s already completed, skipping", worker_id)
            return cp_info.get("rows_so_far", 0)

        start_page = pages[0]
        if cp_info and cp_info.get("status") == "in_progress":
            last_page = cp_info.get("checkpoint_value", {}).get("last_page", 0)
            start_page = last_page + 1
            logger.info("Worker %s resuming from page %d", worker_id, start_page)

        worker_rows = cp_info.get("rows_so_far", 0) if cp_info else 0

        for page_num in pages:
            if page_num < start_page:
                continue

            spec = asset.build_request(context, checkpoint={"page": page_num})
            data = client.request(spec)
            df, _ = asset.parse_response(data)
            worker_rows += write_to_temp(engine, temp_table, df)

            save_checkpoint(
                engine,
                run_id=context.run_id,
                asset_name=asset.name,
                worker_id=worker_id,
                checkpoint_type="page",
                checkpoint_value={"last_page": page_num},
                rows_so_far=worker_rows,
                status="in_progress",
            )

        save_checkpoint(
            engine,
            run_id=context.run_id,
            asset_name=asset.name,
            worker_id=worker_id,
            checkpoint_type="page",
            checkpoint_value={"last_page": pages[-1]},
            rows_so_far=worker_rows,
            status="completed",
        )
        return worker_rows

    with ThreadPoolExecutor(max_workers=asset.max_workers) as pool:
        futures = {}
        for idx, pages in enumerate(partitions):
            wid = f"pages_{pages[0]}_{pages[-1]}"
            futures[pool.submit(worker, pages, wid)] = wid

        for future in as_completed(futures):
            wid = futures[future]
            try:
                rows_total += future.result()
            except Exception:
                logger.exception("Worker %s failed", wid)
                # Cancel remaining workers
                pool.shutdown(wait=False, cancel_futures=True)
                raise

    return rows_total


def extract_entity_parallel(
    asset: Any,  # APIAsset
    client: APIClient,
    engine: Engine,
    temp_table: str,
    context: RunContext,
    entity_keys: list[Any],
    existing_checkpoints: dict[str, dict] | None = None,
) -> int:
    """Entity-parallel extraction: fan out parent entity keys across threads.

    Returns total rows extracted.
    """
    existing_checkpoints = existing_checkpoints or {}

    chunk_size = max(1, math.ceil(len(entity_keys) / asset.max_workers))
    partitions = [
        entity_keys[i : i + chunk_size]
        for i in range(0, len(entity_keys), chunk_size)
    ]

    def worker(entities: list[Any], worker_id: str) -> int:
        cp_info = existing_checkpoints.get(worker_id)
        if cp_info and cp_info.get("status") == "completed":
            logger.info("Worker %s already completed, skipping", worker_id)
            return cp_info.get("rows_so_far", 0)

        completed_entities: set = set()
        resume_entity = None
        resume_checkpoint: dict | None = None

        if cp_info and cp_info.get("status") == "in_progress":
            completed_entities = set(
                cp_info.get("checkpoint_value", {}).get("completed_entities", [])
            )
            resume_entity = cp_info.get("checkpoint_value", {}).get("current_entity")
            resume_checkpoint = cp_info.get("checkpoint_value", {}).get(
                "pagination_state"
            )

        worker_rows = cp_info.get("rows_so_far", 0) if cp_info else 0

        for entity_key in entities:
            entity_str = str(entity_key)
            if entity_str in completed_entities:
                continue

            # Get checkpoint for pagination within this entity
            cp = None
            if entity_str == resume_entity and resume_checkpoint:
                cp = resume_checkpoint
                resume_entity = None  # Only apply once

            # Paginate through all pages for this entity
            page_cp = cp
            while True:
                spec = asset.build_entity_request(entity_key, context, checkpoint=page_cp)
                data = client.request(spec)
                df, state = asset.parse_response(data)
                worker_rows += write_to_temp(engine, temp_table, df)

                if not state.has_more:
                    break

                page_cp = {
                    "cursor": state.cursor,
                    "next_offset": state.next_offset,
                    "next_page": state.next_page,
                }

                # Save checkpoint mid-entity
                save_checkpoint(
                    engine,
                    run_id=context.run_id,
                    asset_name=asset.name,
                    worker_id=worker_id,
                    checkpoint_type="entity",
                    checkpoint_value={
                        "completed_entities": list(completed_entities),
                        "current_entity": entity_str,
                        "pagination_state": page_cp,
                    },
                    rows_so_far=worker_rows,
                    status="in_progress",
                )

            completed_entities.add(entity_str)
            save_checkpoint(
                engine,
                run_id=context.run_id,
                asset_name=asset.name,
                worker_id=worker_id,
                checkpoint_type="entity",
                checkpoint_value={"completed_entities": list(completed_entities)},
                rows_so_far=worker_rows,
                status="in_progress",
            )

        save_checkpoint(
            engine,
            run_id=context.run_id,
            asset_name=asset.name,
            worker_id=worker_id,
            checkpoint_type="entity",
            checkpoint_value={"completed_entities": list(completed_entities)},
            rows_so_far=worker_rows,
            status="completed",
        )
        return worker_rows

    with ThreadPoolExecutor(max_workers=asset.max_workers) as pool:
        futures = {}
        for idx, entities in enumerate(partitions):
            wid = f"entities_{idx}"
            futures[pool.submit(worker, entities, wid)] = wid

        total_rows = 0
        for future in as_completed(futures):
            wid = futures[future]
            try:
                total_rows += future.result()
            except Exception:
                logger.exception("Worker %s failed", wid)
                pool.shutdown(wait=False, cancel_futures=True)
                raise

    return total_rows


def extract_sequential(
    asset: Any,  # APIAsset
    client: APIClient,
    engine: Engine,
    temp_table: str,
    context: RunContext,
    checkpoint: dict | None = None,
) -> int:
    """Sequential extraction with pagination and checkpoint support.

    Each iteration calls asset.build_request() with the latest checkpoint,
    giving the asset full control over URL and params (important for assets
    like GitHubRepos that iterate through multiple API endpoints).

    Returns total rows extracted.
    """
    rows_total = checkpoint.get("rows_so_far", 0) if checkpoint else 0
    cp = checkpoint.get("checkpoint_value") if checkpoint else None

    spec = asset.build_request(context, checkpoint=cp)
    data = client.request(spec)
    df, state = asset.parse_response(data)
    rows_total += write_to_temp(engine, temp_table, df)

    while state.has_more:
        cp_value = {
            "cursor": state.cursor,
            "next_offset": state.next_offset,
            "next_page": state.next_page,
        }
        save_checkpoint(
            engine,
            run_id=context.run_id,
            asset_name=asset.name,
            worker_id="main",
            checkpoint_type=asset.pagination_config.strategy,
            checkpoint_value=cp_value,
            rows_so_far=rows_total,
        )

        # Always call build_request() so the asset controls URL + params.
        # This supports assets that change endpoints mid-extraction (e.g.,
        # iterating through multiple orgs).
        spec = asset.build_request(context, checkpoint=cp_value)
        data = client.request(spec)
        df, state = asset.parse_response(data)
        rows_total += write_to_temp(engine, temp_table, df)

    return rows_total
