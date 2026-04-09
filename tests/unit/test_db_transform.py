"""Tests for transform.db_transform: SQL-based transforms with temp table output."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from data_assets.transform.db_transform import execute_transform
from tests.unit.conftest import make_ctx


# ---------------------------------------------------------------------------
# execute_transform
# ---------------------------------------------------------------------------


class TestExecuteTransform:
    def test_returns_row_count(self):
        """execute_transform should return the number of rows written."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        df = pd.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        ctx = make_ctx(asset_name="my_transform")

        with patch("data_assets.transform.db_transform.pd.read_sql", return_value=df):
            with patch(
                "data_assets.transform.db_transform.write_to_temp", return_value=3
            ) as mock_write:
                result = execute_transform(
                    mock_engine, "SELECT * FROM src", "tmp_table", ctx
                )

        assert result == 3
        mock_write.assert_called_once_with(mock_engine, "tmp_table", df)

    def test_sets_statement_timeout(self):
        """execute_transform should SET LOCAL statement_timeout before running query."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        df = pd.DataFrame({"id": [1]})
        ctx = make_ctx(asset_name="timeout_test")

        with patch("data_assets.transform.db_transform.pd.read_sql", return_value=df):
            with patch(
                "data_assets.transform.db_transform.write_to_temp", return_value=1
            ):
                execute_transform(
                    mock_engine, "SELECT 1", "tmp", ctx, timeout_seconds=600
                )

        # First call on the connection should be the timeout
        timeout_call = mock_conn.execute.call_args_list[0]
        assert "600s" in str(timeout_call.args[0].text)

    def test_passes_query_to_read_sql(self):
        """The SQL query should be passed to pd.read_sql along with the connection."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        df = pd.DataFrame({"x": [1]})
        ctx = make_ctx(asset_name="query_test")
        query = "SELECT x FROM some_table WHERE id > 10"

        with patch(
            "data_assets.transform.db_transform.pd.read_sql", return_value=df
        ) as mock_read:
            with patch(
                "data_assets.transform.db_transform.write_to_temp", return_value=1
            ):
                execute_transform(mock_engine, query, "tmp", ctx)

        mock_read.assert_called_once_with(query, mock_conn)

    def test_empty_result(self):
        """When the query returns zero rows, write_to_temp should still be called."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        df = pd.DataFrame()
        ctx = make_ctx(asset_name="empty_test")

        with patch("data_assets.transform.db_transform.pd.read_sql", return_value=df):
            with patch(
                "data_assets.transform.db_transform.write_to_temp", return_value=0
            ) as mock_write:
                result = execute_transform(
                    mock_engine, "SELECT 1 WHERE false", "tmp", ctx
                )

        assert result == 0
        mock_write.assert_called_once()
