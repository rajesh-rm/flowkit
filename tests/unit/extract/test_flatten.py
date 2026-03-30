"""Tests for flatten_record() and pick_fields() utilities."""

from data_assets.extract.flatten import flatten_record, pick_fields

# --- flatten_record ---

def test_flat_dict_unchanged():
    assert flatten_record({"a": 1, "b": 2}) == {"a": 1, "b": 2}


def test_one_level_nesting():
    result = flatten_record({"user": {"login": "alice", "id": 42}})
    assert result == {"user__login": "alice", "user__id": 42}


def test_two_level_nesting():
    result = flatten_record({"a": {"b": {"c": 3}}}, max_depth=2)
    assert result == {"a__b__c": 3}


def test_max_depth_stops_flattening():
    result = flatten_record({"a": {"b": {"c": {"d": 4}}}}, max_depth=1)
    assert result["a__b"] == {"c": {"d": 4}}


def test_custom_separator():
    result = flatten_record({"user": {"name": "alice"}}, sep=".")
    assert result == {"user.name": "alice"}


def test_lists_are_not_flattened():
    result = flatten_record({"tags": ["a", "b"], "meta": {"x": 1}})
    assert result["tags"] == ["a", "b"]
    assert result["meta__x"] == 1


def test_none_values_preserved():
    result = flatten_record({"a": None, "b": {"c": None}})
    assert result["a"] is None
    assert result["b__c"] is None


def test_empty_dict():
    assert flatten_record({}) == {}


# --- pick_fields ---

def test_pick_top_level():
    record = {"id": 42, "name": "alice"}
    result = pick_fields(record, {"user_id": "id", "user_name": "name"})
    assert result == {"user_id": 42, "user_name": "alice"}


def test_pick_nested():
    record = {"user": {"login": "alice"}, "head": {"ref": "main"}}
    result = pick_fields(record, {
        "user_login": "user.login",
        "head_ref": "head.ref",
    })
    assert result == {"user_login": "alice", "head_ref": "main"}


def test_pick_missing_returns_none():
    record = {"user": {"login": "alice"}}
    result = pick_fields(record, {"email": "user.email"})
    assert result == {"email": None}


def test_pick_deeply_nested():
    record = {"a": {"b": {"c": {"d": "deep"}}}}
    result = pick_fields(record, {"value": "a.b.c.d"})
    assert result == {"value": "deep"}


def test_pick_from_github_pr():
    """Real-world: extract from a GitHub PR response."""
    pr = {
        "id": 500,
        "number": 42,
        "user": {"login": "dev-alice"},
        "head": {"ref": "feature/x"},
        "base": {"ref": "main", "repo": {"full_name": "org/repo"}},
    }
    result = pick_fields(pr, {
        "id": "id",
        "number": "number",
        "user_login": "user.login",
        "head_ref": "head.ref",
        "base_ref": "base.ref",
        "repo_full_name": "base.repo.full_name",
    })
    assert result["user_login"] == "dev-alice"
    assert result["repo_full_name"] == "org/repo"
