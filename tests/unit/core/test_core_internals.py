"""Tests for core internals: enums, types, identifiers, and registry."""

from __future__ import annotations

import time
import uuid

import pytest

from data_assets.core.asset import Asset
from data_assets.core.column import Column
from data_assets.core.enums import (
    CheckpointType,
    LoadStrategy,
    ParallelMode,
    RunMode,
    SchemaContract,
)
from data_assets.core.identifiers import uuid7
from data_assets.core.registry import all_assets, get, register
from data_assets.core.run_context import RunContext
from sqlalchemy import Integer

from data_assets.core.types import (
    PaginationConfig,
    PaginationState,
    RequestSpec,
    ValidationResult,
)


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


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


def test_run_mode_from_string():
    assert RunMode("full") == RunMode.FULL
    assert RunMode("forward") == RunMode.FORWARD


def test_schema_contract_from_string():
    assert SchemaContract("evolve") == SchemaContract.EVOLVE
    assert SchemaContract("freeze") == SchemaContract.FREEZE
    assert SchemaContract("discard") == SchemaContract.DISCARD


def test_checkpoint_type_values():
    assert CheckpointType.SEQUENTIAL.value == "sequential"
    assert CheckpointType.PAGE_PARALLEL.value == "page_parallel"
    assert CheckpointType.ENTITY_PARALLEL.value == "entity_parallel"


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


def test_column_frozen():
    col = Column(name="id", pg_type="INTEGER", nullable=False)
    assert col.name == "id"
    assert col.pg_type == "INTEGER"
    assert col.nullable is False
    assert col.default is None


def test_run_context_frozen():
    ctx = RunContext(
        run_id=uuid.uuid4(),
        mode=RunMode.FULL,
        asset_name="test",
    )
    assert ctx.start_date is None
    assert ctx.params == {}


def test_pagination_config():
    cfg = PaginationConfig(strategy="cursor", page_size=50, cursor_field="next")
    assert cfg.strategy == "cursor"
    assert cfg.page_size == 50


def test_request_spec():
    spec = RequestSpec(method="GET", url="https://api.example.com/data")
    assert spec.params is None
    assert spec.body is None


def test_pagination_state():
    state = PaginationState(has_more=True, cursor="abc123")
    assert state.has_more is True
    assert state.next_offset is None


def test_validation_result_passed():
    result = ValidationResult(passed=True)
    assert result.failures == []


def test_validation_result_failed():
    result = ValidationResult(passed=False, failures=["No rows"])
    assert not result.passed
    assert len(result.failures) == 1


# ---------------------------------------------------------------------------
# Identifiers (UUIDv7)
# ---------------------------------------------------------------------------


def test_uuid7_returns_valid_uuid():
    result = uuid7()
    assert isinstance(result, uuid.UUID)


def test_uuid7_version_is_7():
    result = uuid7()
    assert result.version == 7


def test_uuid7_uniqueness():
    ids = {uuid7() for _ in range(100)}
    assert len(ids) == 100


def test_uuid7_sortable_by_time():
    """UUIDv7s generated in sequence should sort chronologically."""
    first = uuid7()
    time.sleep(0.002)  # 2ms — enough for different millisecond timestamp
    second = uuid7()
    assert str(first) < str(second)


def test_uuid7_embeds_timestamp():
    """The first 48 bits should encode a recent Unix millisecond timestamp."""
    before_ms = int(time.time() * 1000)
    result = uuid7()
    after_ms = int(time.time() * 1000)

    # Extract the 48-bit timestamp from the UUID
    extracted_ms = result.int >> 80
    assert before_ms <= extracted_ms <= after_ms


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


def _make_asset(name: str) -> type[Asset]:
    """Create a minimal concrete asset class."""
    return type(name, (Asset,), {
        "name": name,
        "target_table": name,
        "columns": [Column("id", Integer(), nullable=False)],
        "primary_key": ["id"],
        "load_strategy": LoadStrategy.FULL_REPLACE,
        "default_run_mode": RunMode.FULL,
    })


def test_register_and_get():
    cls = _make_asset("test_asset")
    register(cls)
    assert get("test_asset") is cls


def test_get_unknown_raises():
    with pytest.raises(KeyError, match="not_registered"):
        get("not_registered")


def test_all_assets():
    cls1 = _make_asset("asset_a")
    cls2 = _make_asset("asset_b")
    register(cls1)
    register(cls2)
    assets = all_assets()
    assert "asset_a" in assets
    assert "asset_b" in assets


def test_register_overwrites_duplicate():
    cls1 = _make_asset("dup")
    cls2 = _make_asset("dup")
    register(cls1)
    register(cls2)
    assert get("dup") is cls2


def test_register_as_decorator():
    @register
    class MyAsset(Asset):
        name = "decorated"
        target_table = "decorated"
        columns = [Column("id", Integer(), nullable=False)]
        primary_key = ["id"]

    assert get("decorated") is MyAsset
