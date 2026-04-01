"""Tests for core enum types."""

from data_assets.core.enums import (
    LoadStrategy,
    PaginationStrategy,
    ParallelMode,
    RunMode,
    SchemaContract,
)


def test_run_mode_values():
    assert RunMode.FULL.value == "full"
    assert RunMode.FORWARD.value == "forward"
    assert RunMode.BACKFILL.value == "backfill"
    assert RunMode.TRANSFORM.value == "transform"


def test_load_strategy_values():
    assert LoadStrategy.FULL_REPLACE.value == "full_replace"
    assert LoadStrategy.UPSERT.value == "upsert"
    assert LoadStrategy.APPEND.value == "append"


def test_parallel_mode_values():
    assert ParallelMode.NONE.value == "none"
    assert ParallelMode.PAGE_PARALLEL.value == "page_parallel"
    assert ParallelMode.ENTITY_PARALLEL.value == "entity_parallel"


def test_schema_contract_values():
    assert SchemaContract.EVOLVE.value == "evolve"
    assert SchemaContract.FREEZE.value == "freeze"
    assert SchemaContract.DISCARD.value == "discard"


def test_pagination_strategy_values():
    assert PaginationStrategy.PAGE_NUMBER.value == "page_number"
    assert PaginationStrategy.OFFSET.value == "offset"
    assert PaginationStrategy.CURSOR.value == "cursor"
    assert PaginationStrategy.KEYSET.value == "keyset"
    assert PaginationStrategy.NONE.value == "none"


def test_run_mode_from_string():
    assert RunMode("full") == RunMode.FULL
    assert RunMode("forward") == RunMode.FORWARD


def test_schema_contract_from_string():
    assert SchemaContract("evolve") == SchemaContract.EVOLVE
    assert SchemaContract("freeze") == SchemaContract.FREEZE
    assert SchemaContract("discard") == SchemaContract.DISCARD


def test_pagination_strategy_from_string():
    assert PaginationStrategy("page_number") == PaginationStrategy.PAGE_NUMBER
    assert PaginationStrategy("offset") == PaginationStrategy.OFFSET
    assert PaginationStrategy("cursor") == PaginationStrategy.CURSOR
    assert PaginationStrategy("none") == PaginationStrategy.NONE
