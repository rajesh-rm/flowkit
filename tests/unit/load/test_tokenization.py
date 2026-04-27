"""Tests for apply_tokenization: dedup, mapping back, NULL handling."""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd

from data_assets.load.tokenization import apply_tokenization


def _client(map_fn):
    """Build a stub client whose .tokenize(values) calls map_fn for each value."""
    client = MagicMock()
    client.tokenize = MagicMock(side_effect=lambda vals: [map_fn(v) for v in vals])
    return client


class TestApplyTokenization:

    def test_deduplicates_before_calling_endpoint(self):
        df = pd.DataFrame({"user": ["alice", "bob", "alice", "alice", "bob"]})
        client = _client(lambda v: f"tok_{v}")

        apply_tokenization(df, ["user"], client)

        # API called once per column with deduped list (preserving first-seen order).
        assert client.tokenize.call_count == 1
        called_with = client.tokenize.call_args[0][0]
        assert called_with == ["alice", "bob"]

    def test_remaps_all_occurrences_including_duplicates(self):
        df = pd.DataFrame({"user": ["alice", "bob", "alice", "alice", "bob"]})
        client = _client(lambda v: f"tok_{v}")

        result = apply_tokenization(df, ["user"], client)

        assert result["user"].tolist() == [
            "tok_alice", "tok_bob", "tok_alice", "tok_alice", "tok_bob",
        ]

    def test_nulls_pass_through_unchanged(self):
        df = pd.DataFrame({"user": ["alice", None, "bob", None]})
        client = _client(lambda v: f"tok_{v}")

        result = apply_tokenization(df, ["user"], client)

        # API receives only non-null deduped values.
        assert client.tokenize.call_args[0][0] == ["alice", "bob"]
        # Nulls survive.
        assert result["user"].iloc[0] == "tok_alice"
        assert pd.isna(result["user"].iloc[1])
        assert result["user"].iloc[2] == "tok_bob"
        assert pd.isna(result["user"].iloc[3])

    def test_all_null_column_skips_api_call(self):
        df = pd.DataFrame({"user": [None, None, None]})
        client = _client(lambda v: f"tok_{v}")

        apply_tokenization(df, ["user"], client)

        client.tokenize.assert_not_called()

    def test_empty_dataframe_no_api_call(self):
        df = pd.DataFrame({"user": []})
        client = _client(lambda v: f"tok_{v}")

        apply_tokenization(df, ["user"], client)

        client.tokenize.assert_not_called()

    def test_missing_column_silently_skipped(self):
        # An asset may declare sensitive_columns that aren't all present in
        # every parse_response (e.g., partial pages from an API).
        df = pd.DataFrame({"other": ["x", "y"]})
        client = _client(lambda v: f"tok_{v}")

        result = apply_tokenization(df, ["user"], client)

        client.tokenize.assert_not_called()
        assert "user" not in result.columns

    def test_multiple_sensitive_columns_each_called(self):
        df = pd.DataFrame({
            "user": ["alice", "bob"],
            "email": ["a@x.com", "b@x.com"],
            "company": ["Acme", "Beta"],
        })
        client = _client(lambda v: f"T{v}")

        apply_tokenization(df, ["user", "email"], client)

        assert client.tokenize.call_count == 2
        assert df["user"].tolist() == ["Talice", "Tbob"]
        assert df["email"].tolist() == ["Ta@x.com", "Tb@x.com"]
        # Non-sensitive column untouched.
        assert df["company"].tolist() == ["Acme", "Beta"]

    def test_numeric_values_stringified_before_send(self):
        df = pd.DataFrame({"user_id": [1001, 1002, 1001]})
        client = _client(lambda v: f"tok_{v}")

        apply_tokenization(df, ["user_id"], client)

        # API receives string form of unique ints.
        assert client.tokenize.call_args[0][0] == ["1001", "1002"]
        assert df["user_id"].tolist() == ["tok_1001", "tok_1002", "tok_1001"]

    def test_returns_same_dataframe_for_chaining(self):
        df = pd.DataFrame({"user": ["alice"]})
        client = _client(lambda v: f"tok_{v}")

        result = apply_tokenization(df, ["user"], client)

        assert result is df

    def test_no_sensitive_columns_no_api_call(self):
        df = pd.DataFrame({"user": ["alice", "bob"]})
        client = _client(lambda v: f"tok_{v}")

        apply_tokenization(df, [], client)

        client.tokenize.assert_not_called()
