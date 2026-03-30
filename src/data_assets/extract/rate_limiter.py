"""In-process token-bucket rate limiter (thread-safe, with jitter)."""

from __future__ import annotations

import logging
import random
import threading
import time

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe token-bucket rate limiter.

    Shared across all threads in a parallel extraction so that
    the total outbound call rate is enforced globally.

    Includes jitter to prevent thundering herd when multiple workers
    wake up simultaneously after a pause.
    """

    def __init__(self, rate_per_second: float) -> None:
        self._rate = rate_per_second
        self._max_tokens = rate_per_second
        self._tokens = rate_per_second
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._retry_after_until: float = 0.0

    def acquire(self) -> None:
        """Block until a token is available, then consume one."""
        while True:
            got_token = False
            with self._lock:
                now = time.monotonic()
                if now < self._retry_after_until:
                    pause = self._retry_after_until - now
                else:
                    pause = 0.0

                if pause > 0:
                    pass  # Will sleep after releasing lock
                else:
                    elapsed = now - self._last_refill
                    self._tokens = min(
                        self._max_tokens, self._tokens + elapsed * self._rate
                    )
                    self._last_refill = now

                    if self._tokens >= 1.0:
                        self._tokens -= 1.0
                        got_token = True
                    else:
                        pause = (1.0 - self._tokens) / self._rate

            # Outside the lock
            if got_token:
                # Small jitter to spread concurrent wakeups
                jitter = random.uniform(0, 0.02 / max(self._rate, 0.1))
                if jitter > 0.0005:
                    time.sleep(jitter)
                return

            if pause > 0:
                time.sleep(pause)

    def pause_for(self, seconds: float) -> None:
        """Pause all acquisitions for the given duration (Retry-After)."""
        with self._lock:
            deadline = time.monotonic() + seconds
            self._retry_after_until = max(self._retry_after_until, deadline)
            logger.info("Rate limiter paused for %.1f seconds", seconds)
