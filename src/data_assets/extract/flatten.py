"""Utility for extracting values from nested API response dicts."""

from __future__ import annotations

from typing import Any


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
