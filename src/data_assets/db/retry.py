"""Database operation retry with exponential backoff.

Retries only on transient connection errors (OperationalError,
DisconnectionError). Data errors (IntegrityError, ProgrammingError)
fail immediately — retrying won't help.

Designed for Airflow admins and junior devs: each retry logs a clear
WARNING, and exhaustion logs an ERROR with an actionable message.
"""

from __future__ import annotations

import functools
import logging
import os
import time

from sqlalchemy.exc import DisconnectionError, IntegrityError, OperationalError, ProgrammingError

logger = logging.getLogger(__name__)

_RETRYABLE = (OperationalError, DisconnectionError, ConnectionError, TimeoutError)
_NON_RETRYABLE = (IntegrityError, ProgrammingError)


class DatabaseRetryExhausted(Exception):
    """All database retry attempts failed.

    Wraps the last underlying exception so callers can distinguish DB
    connectivity failures from API or data errors.
    """

    def __init__(self, attempts: int, last_error: Exception):
        self.attempts = attempts
        self.last_error = last_error
        super().__init__(
            f"Database connection failed after {attempts} attempts. "
            f"Last error: {last_error}"
        )


def _resolve_config(
    max_attempts: int | None, base_delay: float | None,
) -> tuple[int, float]:
    """Resolve retry config from explicit args or env vars."""
    attempts = max_attempts if max_attempts is not None else int(
        os.environ.get("DATA_ASSETS_DB_RETRY_ATTEMPTS", "3")
    )
    delay = base_delay if base_delay is not None else float(
        os.environ.get("DATA_ASSETS_DB_RETRY_BASE_DELAY", "2.0")
    )
    return attempts, delay


def _execute_with_retry(fn, args, kwargs, attempts, base_delay, max_delay):
    """Run fn with retry on transient DB errors. Raises on exhaustion."""
    last_exc = None
    total_wait = 0.0

    for attempt in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except _NON_RETRYABLE:
            raise
        except _RETRYABLE as exc:
            last_exc = exc
            if attempt == attempts:
                break
            wait = min(base_delay * (2 ** (attempt - 1)), max_delay)
            total_wait += wait
            logger.warning(
                "Database write failed (attempt %d/%d): %s. "
                "Retrying in %.1fs...",
                attempt, attempts, exc, wait,
            )
            time.sleep(wait)

    logger.error(
        "Database connection failed after %d attempts "
        "(total wait: %.1fs). Last error: %s. "
        "Action: Check database connectivity, verify credentials, "
        "and restart the Airflow task.",
        attempts, total_wait, last_exc,
    )
    raise DatabaseRetryExhausted(attempts, last_exc) from last_exc


def db_retry(
    max_attempts: int | None = None,
    base_delay: float | None = None,
    max_delay: float = 30.0,
):
    """Decorator: retry a function on transient database errors.

    Args:
        max_attempts: Total attempts (including the first). Default 3,
            overridable via DATA_ASSETS_DB_RETRY_ATTEMPTS env var.
        base_delay: Initial backoff in seconds. Default 2.0, overridable
            via DATA_ASSETS_DB_RETRY_BASE_DELAY env var.
        max_delay: Backoff cap in seconds.
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            attempts, delay = _resolve_config(max_attempts, base_delay)
            return _execute_with_retry(fn, args, kwargs, attempts, delay, max_delay)

        return wrapper

    return decorator
