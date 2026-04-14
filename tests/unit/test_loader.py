"""Unit tests for loader coercion utilities."""

from __future__ import annotations

import pandas as pd

from data_assets.load.loader import _coerce_datetime_strings


class TestCoerceDatetimeStrings:
    def test_iso8601_detected(self):
        df = pd.DataFrame({"ts": ["2025-12-01T10:00:00Z", "2025-12-02T11:00:00Z"]})
        result = _coerce_datetime_strings(df)
        assert result["ts"].dtype.name.startswith("datetime")

    def test_servicenow_format_detected(self):
        df = pd.DataFrame({"ts": ["2025-12-01 10:00:00", "2025-12-02 11:00:00"]})
        result = _coerce_datetime_strings(df)
        assert result["ts"].dtype.name.startswith("datetime")

    def test_empty_strings_become_nat(self):
        df = pd.DataFrame({"ts": ["2025-12-01 10:00:00", "", "2025-12-02 11:00:00"]})
        result = _coerce_datetime_strings(df)
        assert pd.isna(result["ts"].iloc[1])
        assert not pd.isna(result["ts"].iloc[0])

    def test_all_empty_strings_no_crash(self):
        df = pd.DataFrame({"ts": ["", "", ""]})
        result = _coerce_datetime_strings(df)
        assert not result["ts"].dtype.name.startswith("datetime")

    def test_plain_text_not_detected(self):
        df = pd.DataFrame({"name": ["alice", "bob", "charlie"]})
        result = _coerce_datetime_strings(df)
        assert not result["name"].dtype.name.startswith("datetime")

    def test_date_only_not_detected(self):
        df = pd.DataFrame({"dt": ["2025-12-01", "2025-12-02"]})
        result = _coerce_datetime_strings(df)
        assert not result["dt"].dtype.name.startswith("datetime")

    def test_non_string_columns_skipped(self):
        df = pd.DataFrame({"val": [1, 2, 3]})
        result = _coerce_datetime_strings(df)
        assert result["val"].dtype in ("int64", "int32")

    def test_mixed_valid_and_empty(self):
        df = pd.DataFrame({"ts": ["", "2025-12-01 10:00:00", ""]})
        result = _coerce_datetime_strings(df)
        assert result["ts"].dtype.name.startswith("datetime")
        assert pd.isna(result["ts"].iloc[0])
        assert pd.isna(result["ts"].iloc[2])
        assert not pd.isna(result["ts"].iloc[1])
