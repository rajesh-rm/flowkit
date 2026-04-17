"""Unit tests for the missing-key validation module."""

from __future__ import annotations

import pytest

from data_assets.validation.missing_keys import (
    MissingKeyError,
    _present_in,
    check_required_keys,
)


# ---------------------------------------------------------------------------
# _present_in
# ---------------------------------------------------------------------------


def test_present_in_top_level_present():
    assert _present_in({"a": 1}, "a") is True


def test_present_in_top_level_absent():
    assert _present_in({"a": 1}, "b") is False


def test_present_in_top_level_value_none_counts_as_present():
    """Key present with value None is semantically distinct from absent."""
    assert _present_in({"a": None}, "a") is True


def test_present_in_nested_present():
    assert _present_in({"a": {"b": {"c": 42}}}, "a.b.c") is True


def test_present_in_nested_absent_mid_segment():
    assert _present_in({"a": {"b": {}}}, "a.b.c") is False


def test_present_in_nested_absent_final_segment():
    assert _present_in({"a": {"b": 1}}, "a.b.c") is False


def test_present_in_nested_intermediate_is_none():
    """A None intermediate is treated as present-as-null.

    The API explicitly acknowledged "no sub-entity here" (e.g., Jira
    ``fields.assignee: null``). Subsequent path segments are considered
    present-as-null rather than missing.
    """
    assert _present_in({"a": None}, "a.b") is True


def test_present_in_nested_empty_dict_child_is_absent():
    """An empty-dict intermediate still flags the missing child.

    Ensures the null-intermediate relaxation is bounded: an explicit null
    parent is tolerant, but a present-but-empty parent still requires the
    declared child key.
    """
    assert _present_in({"a": {}}, "a.b") is False


def test_present_in_deeper_path_with_null_mid_segment():
    """Null at any intermediate short-circuits to present-as-null."""
    assert _present_in({"a": {"b": None}}, "a.b.c") is True
    assert _present_in({"a": None}, "a.b.c") is True


def test_present_in_non_dict_intermediate_returns_absent():
    """Non-dict non-None intermediates signal a malformed response.

    Unlike explicit null, a scalar where a dict was expected indicates a
    broken contract — the walker returns False so the missing-key check
    surfaces the anomaly.
    """
    assert _present_in({"a": "string"}, "a.b") is False
    assert _present_in({"a": 5}, "a.b") is False
    assert _present_in({"a": []}, "a.b") is False
    assert _present_in({"a": True}, "a.b") is False


def test_present_in_value_is_not_dict():
    """Walking past a scalar yields absent."""
    assert _present_in({"a": 5}, "a.b") is False


def test_present_in_empty_record():
    assert _present_in({}, "a") is False


# ---------------------------------------------------------------------------
# check_required_keys
# ---------------------------------------------------------------------------


def test_check_required_keys_all_present_passes():
    records = [{"id": 1, "name": "x"}, {"id": 2, "name": "y"}]
    check_required_keys(records, {"id": "id", "name": "name"}, [], "asset")


def test_check_required_keys_missing_raises():
    records = [{"id": 1}]
    with pytest.raises(MissingKeyError) as exc_info:
        check_required_keys(
            records, {"id": "id", "name": "name"}, [], "my_asset",
        )
    err = exc_info.value
    assert err.asset_name == "my_asset"
    assert err.column == "name"
    assert err.field_path == "name"
    assert err.record_index == 0


def test_check_required_keys_reports_first_offender():
    """First offending record should be flagged, not the last."""
    records = [
        {"id": 1, "name": "x"},
        {"id": 2},  # missing 'name'
        {"id": 3, "name": "z"},
    ]
    with pytest.raises(MissingKeyError) as exc_info:
        check_required_keys(
            records, {"id": "id", "name": "name"}, [], "asset",
        )
    assert exc_info.value.record_index == 1


def test_check_required_keys_optional_skipped():
    records = [{"id": 1}]  # missing 'name' but it's optional
    check_required_keys(
        records, {"id": "id", "name": "name"}, ["name"], "asset",
    )


def test_check_required_keys_null_value_is_not_missing():
    """A key present with value None passes — it was returned by the API."""
    records = [{"id": 1, "name": None}]
    check_required_keys(records, {"id": "id", "name": "name"}, [], "asset")


def test_check_required_keys_nested_path():
    records = [{"id": 1, "user": {"login": "alice"}}]
    check_required_keys(
        records,
        {"id": "id", "user.login": "user_login"},
        [],
        "asset",
    )


def test_check_required_keys_nested_path_missing_raises():
    records = [{"id": 1, "user": {}}]  # user.login absent
    with pytest.raises(MissingKeyError) as exc_info:
        check_required_keys(
            records,
            {"id": "id", "user.login": "user_login"},
            [],
            "asset",
        )
    assert exc_info.value.column == "user_login"
    assert exc_info.value.field_path == "user.login"


def test_check_required_keys_empty_records_passes():
    """No records → nothing to validate, nothing to raise."""
    check_required_keys([], {"id": "id"}, [], "asset")


def test_missing_key_error_message_mentions_optional_hint():
    records = [{"id": 1}]
    with pytest.raises(MissingKeyError) as exc_info:
        check_required_keys(records, {"id": "id", "x": "x"}, [], "asset")
    msg = str(exc_info.value)
    assert "optional_columns" in msg


def test_check_required_keys_passes_when_parent_is_null():
    """A nested required path whose parent is explicit null passes without raising.

    Mirrors real API semantics: Jira returns ``fields.assignee: null`` for
    unassigned issues; GitHub returns ``user: null`` for PRs by deleted
    accounts. These are not schema-drift events and must not block the run.
    """
    records = [{"id": 1, "fields": {"assignee": None}}]
    check_required_keys(
        records,
        {"id": "id", "fields.assignee.displayName": "assignee"},
        [],
        "asset",
    )


def test_check_required_keys_still_catches_empty_dict_parent():
    """Empty-dict parent is NOT a free pass — missing child still raises."""
    records = [{"id": 1, "user": {}}]
    with pytest.raises(MissingKeyError) as exc_info:
        check_required_keys(
            records, {"id": "id", "user.login": "user_login"}, [], "asset",
        )
    assert exc_info.value.column == "user_login"
