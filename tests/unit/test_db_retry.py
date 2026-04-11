"""Tests for the database retry decorator."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy.exc import DisconnectionError, IntegrityError, OperationalError, ProgrammingError

from data_assets.db.retry import DatabaseRetryExhausted, db_retry


class TestDbRetrySuccess:
    def test_success_first_try(self):
        @db_retry(max_attempts=3, base_delay=0.01)
        def fn():
            return "ok"

        assert fn() == "ok"

    def test_success_after_retry(self):
        call_count = 0

        @db_retry(max_attempts=3, base_delay=0.01)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise OperationalError("conn refused", None, None)
            return "recovered"

        assert fn() == "recovered"
        assert call_count == 3

    def test_success_on_second_attempt(self):
        call_count = 0

        @db_retry(max_attempts=3, base_delay=0.01)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise DisconnectionError("lost connection")
            return "ok"

        assert fn() == "ok"
        assert call_count == 2


class TestDbRetryExhaustion:
    def test_raises_after_all_attempts(self):
        @db_retry(max_attempts=2, base_delay=0.01)
        def fn():
            raise OperationalError("timeout", None, None)

        with pytest.raises(DatabaseRetryExhausted) as exc_info:
            fn()

        assert exc_info.value.attempts == 2
        assert isinstance(exc_info.value.last_error, OperationalError)

    def test_connection_error_retried_and_exhausted(self):
        @db_retry(max_attempts=2, base_delay=0.01)
        def fn():
            raise ConnectionRefusedError("Connection refused")

        with pytest.raises(DatabaseRetryExhausted):
            fn()

    def test_timeout_error_retried_and_exhausted(self):
        @db_retry(max_attempts=2, base_delay=0.01)
        def fn():
            raise TimeoutError("socket timed out")

        with pytest.raises(DatabaseRetryExhausted):
            fn()

    def test_file_not_found_not_retried(self):
        call_count = 0

        @db_retry(max_attempts=3, base_delay=0.01)
        def fn():
            nonlocal call_count
            call_count += 1
            raise FileNotFoundError("/var/run/postgresql/.s.PGSQL.5432")

        with pytest.raises(FileNotFoundError):
            fn()
        assert call_count == 1  # no retry


class TestDbRetryNonRetryable:
    def test_integrity_error_not_retried(self):
        call_count = 0

        @db_retry(max_attempts=3, base_delay=0.01)
        def fn():
            nonlocal call_count
            call_count += 1
            raise IntegrityError("duplicate key", None, None)

        with pytest.raises(IntegrityError):
            fn()
        assert call_count == 1  # no retry

    def test_programming_error_not_retried(self):
        call_count = 0

        @db_retry(max_attempts=3, base_delay=0.01)
        def fn():
            nonlocal call_count
            call_count += 1
            raise ProgrammingError("syntax error", None, None)

        with pytest.raises(ProgrammingError):
            fn()
        assert call_count == 1


class TestDbRetryLogging:
    def test_logs_warning_on_retry(self):
        call_count = 0

        @db_retry(max_attempts=2, base_delay=0.01)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OperationalError("conn refused", None, None)
            return "ok"

        with patch("data_assets.db.retry.logger") as mock_logger:
            fn()
            mock_logger.warning.assert_called_once()
            args = mock_logger.warning.call_args[0]
            assert "attempt" in args[0]
            assert args[1] == 1  # attempt number
            assert args[2] == 2  # total attempts

    def test_logs_error_on_exhaustion(self):
        @db_retry(max_attempts=1, base_delay=0.01)
        def fn():
            raise OperationalError("timeout", None, None)

        with patch("data_assets.db.retry.logger") as mock_logger:
            with pytest.raises(DatabaseRetryExhausted):
                fn()
            mock_logger.error.assert_called_once()
            assert "Action:" in mock_logger.error.call_args[0][0]


class TestDbRetryEnvConfig:
    def test_env_overrides_defaults(self, monkeypatch):
        monkeypatch.setenv("DATA_ASSETS_DB_RETRY_ATTEMPTS", "1")
        monkeypatch.setenv("DATA_ASSETS_DB_RETRY_BASE_DELAY", "0.01")

        @db_retry()
        def fn():
            raise OperationalError("timeout", None, None)

        with pytest.raises(DatabaseRetryExhausted) as exc_info:
            fn()
        assert exc_info.value.attempts == 1


class TestDbRetryPreservesArgs:
    def test_passes_args_and_kwargs(self):
        @db_retry(max_attempts=1, base_delay=0.01)
        def fn(a, b, c=None):
            return (a, b, c)

        assert fn(1, 2, c=3) == (1, 2, 3)
