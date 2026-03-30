"""Tests for _get_nested() utility."""

from data_assets.extract.flatten import _get_nested


def test_get_nested_top_level():
    assert _get_nested({"id": 42}, "id") == 42


def test_get_nested_one_level():
    assert _get_nested({"user": {"login": "alice"}}, "user.login") == "alice"


def test_get_nested_deep():
    obj = {"a": {"b": {"c": {"d": "deep"}}}}
    assert _get_nested(obj, "a.b.c.d") == "deep"


def test_get_nested_missing_key():
    assert _get_nested({"user": {"login": "alice"}}, "user.email") is None


def test_get_nested_missing_intermediate():
    assert _get_nested({"user": None}, "user.login") is None


def test_get_nested_non_dict():
    assert _get_nested("not a dict", "key") is None


def test_get_nested_empty_dict():
    assert _get_nested({}, "key") is None
