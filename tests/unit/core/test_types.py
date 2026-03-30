"""Tests for core types: Column, RunContext, PaginationConfig, etc."""

import uuid

from data_assets.core.column import Column
from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext
from data_assets.core.types import (
    PaginationConfig,
    PaginationState,
    RequestSpec,
    ValidationResult,
)


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
