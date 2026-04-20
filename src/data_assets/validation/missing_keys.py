"""Missing-key detection for API responses.

Pandas collapses two distinct cases into NaN: a key absent from the source
dict, and a key present with value None. Once the DataFrame is built, the
distinction is lost. This module detects missing keys at the raw-dict layer,
before the collapse.

Use from inside `parse_response` implementations (or equivalent pre-DataFrame
hooks like ServiceNow's `_batch_to_df`). Fails fast on the first violation.
"""

from __future__ import annotations


def _present_in(record: dict, dotted_path: str) -> bool:
    """True if every segment of *dotted_path* exists in *record*.

    Treats a segment as "present" even when its value is None — the goal is
    to distinguish absent keys from explicit nulls. If an intermediate segment
    is itself None, the API explicitly acknowledged "no sub-entity" (e.g.,
    Jira returns ``fields.assignee: null`` for unassigned issues); any
    remaining path segments are considered present-as-null. Returns False
    when a segment is genuinely absent or when a non-dict non-None value is
    walked (which signals a malformed response).
    """
    cur = record
    for part in dotted_path.split("."):
        if cur is None:
            return True
        if not isinstance(cur, dict) or part not in cur:
            return False
        cur = cur[part]
    return True


class MissingKeyError(ValueError):
    """A required column's key is absent from an API response record.

    Subclass of ValueError so it flows through the runner's existing exception
    handling (validation failures are raised as ValueError in runner.py).
    """

    def __init__(
        self,
        asset_name: str,
        column: str,
        field_path: str,
        record_index: int,
    ) -> None:
        self.asset_name = asset_name
        self.column = column
        self.field_path = field_path
        self.record_index = record_index
        super().__init__(
            f"Asset '{asset_name}': required column '{column}' "
            f"(API field '{field_path}') is absent from response record "
            f"#{record_index}. If this field is legitimately missing for some "
            f"responses, add '{column}' to the asset's optional_columns list."
        )


def check_required_keys(
    records: list[dict],
    field_to_column: dict[str, str],
    optional_columns: list[str],
    asset_name: str,
) -> None:
    """Raise MissingKeyError for the first non-optional column whose API
    field path is absent from any record.

    Args:
        records: Raw list of dicts as received from the API, before any
            flattening or column selection.
        field_to_column: Map from API field path (dotted for nested) to
            DataFrame column name. Callers must pass an explicit mapping;
            defaulting to identity is unsafe for assets with nested responses.
        optional_columns: Column names exempt from the check.
        asset_name: Used in the error message.
    """
    optional = set(optional_columns)
    for i, record in enumerate(records):
        for field_path, column in field_to_column.items():
            if column in optional:
                continue
            if not _present_in(record, field_path):
                raise MissingKeyError(asset_name, column, field_path, i)
