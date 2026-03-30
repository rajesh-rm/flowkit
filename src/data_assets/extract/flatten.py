"""Utilities for flattening nested API responses into flat records.

Two main functions:
- flatten_record(): recursively flatten a nested dict
- pick_fields(): extract and rename fields using dot-path mapping
"""

from __future__ import annotations

from typing import Any


def flatten_record(
    obj: dict, sep: str = "__", max_depth: int = 2, _prefix: str = "", _depth: int = 0
) -> dict[str, Any]:
    """Recursively flatten a nested dict.

    Args:
        obj: The nested dict to flatten.
        sep: Separator between parent and child keys (default "__").
        max_depth: Maximum nesting depth to flatten. Beyond this, values
                   are kept as-is (dicts/lists stored as JSON-compatible objects).
        _prefix: Internal — accumulated key prefix for recursion.
        _depth: Internal — current recursion depth.

    Returns:
        Flat dict with compound keys, e.g.:
        {"user": {"login": "alice"}} → {"user__login": "alice"}

    Example:
        >>> flatten_record({"a": 1, "b": {"c": 2, "d": {"e": 3}}}, max_depth=2)
        {"a": 1, "b__c": 2, "b__d": {"e": 3}}
    """
    items: dict[str, Any] = {}
    for key, value in obj.items():
        full_key = f"{_prefix}{sep}{key}" if _prefix else key
        if isinstance(value, dict) and _depth < max_depth:
            items.update(
                flatten_record(value, sep=sep, max_depth=max_depth,
                               _prefix=full_key, _depth=_depth + 1)
            )
        else:
            items[full_key] = value
    return items


def pick_fields(record: dict, field_map: dict[str, str]) -> dict[str, Any]:
    """Extract and rename fields from a nested dict using dot-path notation.

    Args:
        record: The source dict (may be nested).
        field_map: Mapping of {output_name: "dot.path.to.value"}.

    Returns:
        Flat dict with output_name keys and extracted values.

    Example:
        >>> pick_fields(
        ...     {"user": {"login": "alice"}, "id": 42},
        ...     {"user_login": "user.login", "pr_id": "id"}
        ... )
        {"user_login": "alice", "pr_id": 42}
    """
    result: dict[str, Any] = {}
    for output_name, dot_path in field_map.items():
        result[output_name] = _get_nested(record, dot_path)
    return result


def _get_nested(obj: Any, dot_path: str) -> Any:
    """Traverse a nested dict using a dot-separated path.

    Returns None if any key in the path is missing.
    """
    current = obj
    for key in dot_path.split("."):
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current
