"""Tests for registry-time validation of asset declarations."""

from __future__ import annotations

import pytest
from sqlalchemy import Integer, Text

from data_assets.core.asset import Asset
from data_assets.core.column import Column, Index
from data_assets.core.registry import (
    _validate_optional_columns,
    _validate_sensitive_data,
)


def _make(cls_dict: dict) -> Asset:
    """Build an anonymous Asset subclass with the given class-level attrs."""
    base = {
        "name": "tmp",
        "target_table": "tmp",
        "columns": [Column("id", Integer(), nullable=False), Column("value", Text())],
        "primary_key": ["id"],
        "indexes": [Index(columns=("value",))],
    }
    base.update(cls_dict)
    NewCls = type("TmpAsset", (Asset,), base)
    return NewCls()


def test_no_optional_columns_is_fine():
    asset = _make({})
    _validate_optional_columns("tmp", asset)  # no raise


def test_optional_column_present_in_schema_passes():
    asset = _make({
        "columns": [
            Column("id", Integer(), nullable=False),
            Column("value", Text()),
            Column("note", Text()),
        ],
        "optional_columns": ["note"],
    })
    _validate_optional_columns("tmp", asset)


def test_unknown_optional_column_raises():
    asset = _make({"optional_columns": ["not_a_column"]})
    with pytest.raises(ValueError, match="unknown columns in optional_columns"):
        _validate_optional_columns("tmp", asset)


def test_pk_column_marked_optional_raises():
    asset = _make({"optional_columns": ["id"]})
    with pytest.raises(ValueError, match="cannot be optional"):
        _validate_optional_columns("tmp", asset)


def test_index_column_marked_optional_raises():
    """`value` is used in an index — can't be optional."""
    asset = _make({"optional_columns": ["value"]})
    with pytest.raises(ValueError, match="cannot be optional"):
        _validate_optional_columns("tmp", asset)


def test_index_include_column_marked_optional_raises():
    asset = _make({
        "columns": [
            Column("id", Integer(), nullable=False),
            Column("value", Text()),
            Column("extra", Text()),
        ],
        "indexes": [Index(columns=("value",), include=("extra",))],
        "optional_columns": ["extra"],
    })
    with pytest.raises(ValueError, match="cannot be optional"):
        _validate_optional_columns("tmp", asset)


def test_error_message_lists_offending_columns():
    asset = _make({
        "columns": [
            Column("id", Integer(), nullable=False),
            Column("value", Text()),
            Column("note", Text()),
        ],
        "optional_columns": ["id", "note"],  # id is PK, note is fine — only id trips
    })
    with pytest.raises(ValueError) as exc_info:
        _validate_optional_columns("tmp", asset)
    assert "'id'" in str(exc_info.value)
    assert "'note'" not in str(exc_info.value)


# ---------------------------------------------------------------------------
# Sensitive-data validation
# ---------------------------------------------------------------------------

class TestSensitiveDataValidation:

    def test_undeclared_flag_rejected(self):
        # Asset with default contains_sensitive_data=None should fail.
        asset = _make({})
        with pytest.raises(ValueError, match="must declare contains_sensitive_data"):
            _validate_sensitive_data("tmp", asset)

    def test_false_with_no_sensitive_columns_passes(self):
        asset = _make({"contains_sensitive_data": False})
        _validate_sensitive_data("tmp", asset)  # no raise

    def test_true_without_any_sensitive_column_rejected(self):
        asset = _make({"contains_sensitive_data": True})
        with pytest.raises(ValueError, match="no columns are marked sensitive"):
            _validate_sensitive_data("tmp", asset)

    def test_true_with_sensitive_column_passes(self):
        asset = _make({
            "contains_sensitive_data": True,
            "columns": [
                Column("id", Integer(), nullable=False),
                Column("user_id", Text(), sensitive=True),
            ],
            "primary_key": ["id"],
            "indexes": [Index(columns=("id",))],
        })
        _validate_sensitive_data("tmp", asset)

    def test_false_with_sensitive_column_rejected(self):
        asset = _make({
            "contains_sensitive_data": False,
            "columns": [
                Column("id", Integer(), nullable=False),
                Column("email", Text(), sensitive=True),
            ],
            "primary_key": ["id"],
            "indexes": [Index(columns=("id",))],
        })
        with pytest.raises(ValueError, match="contains_sensitive_data=False"):
            _validate_sensitive_data("tmp", asset)

    def test_sensitive_column_in_primary_key_is_allowed(self):
        # Per design: the implicit PK index is over tokenized values only.
        asset = _make({
            "contains_sensitive_data": True,
            "columns": [
                Column("user_id", Text(), nullable=False, sensitive=True),
                Column("display", Text()),
            ],
            "primary_key": ["user_id"],
            "indexes": [Index(columns=("display",))],
        })
        _validate_sensitive_data("tmp", asset)

    def test_sensitive_column_in_explicit_index_rejected(self):
        asset = _make({
            "contains_sensitive_data": True,
            "columns": [
                Column("id", Integer(), nullable=False),
                Column("email", Text(), sensitive=True),
            ],
            "primary_key": ["id"],
            "indexes": [Index(columns=("email",))],
        })
        with pytest.raises(ValueError, match="index referencing sensitive columns"):
            _validate_sensitive_data("tmp", asset)

    def test_sensitive_column_in_index_include_rejected(self):
        asset = _make({
            "contains_sensitive_data": True,
            "columns": [
                Column("id", Integer(), nullable=False),
                Column("name", Text()),
                Column("email", Text(), sensitive=True),
            ],
            "primary_key": ["id"],
            "indexes": [Index(columns=("name",), include=("email",))],
        })
        with pytest.raises(ValueError, match="INCLUDE referencing sensitive"):
            _validate_sensitive_data("tmp", asset)
