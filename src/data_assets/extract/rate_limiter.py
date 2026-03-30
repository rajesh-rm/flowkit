"""In-process sliding-window rate limiter (thread-safe).

Tracks timestamps of recent API calls within a 1-second window.
If the window is full (>= rate calls in the last second), blocks
until the oldest call exits the window.

Simpler than token-bucket: no fractional math, no continuous refill.
Just count calls in the window.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque

logger = logging.getLogger(__name__)


class RateLimiter:
    """Thread-safe sliding-window rate limiter.

    Shared across all threads in a parallel extraction so that
    the total outbound call rate is enforced globally.

    How it works:
        - Keeps a deque of timestamps (one per acquire() call)
        - On acquire(), removes expired timestamps (older than 1 second)
        - If fewer than `rate` calls in the window, records the call and returns
        - If window is full, sleeps until the oldest call expires

    Example:
        limiter = RateLimiter(10.0)  # 10 calls per second
        limiter.acquire()            # blocks if 10 calls already made this second
    """

    def __init__(self, rate_per_second: float) -> None:
        self._max_calls = int(max(rate_per_second, 1))
        self._window = 1.0  # 1-second sliding window
        self._timestamps: deque[float] = deque()
        self._lock = threading.Lock()
        self._paused_until: float = 0.0

    def acquire(self) -> None:
        """Block until a call slot is available within the rate window."""
        while True:
            with self._lock:
                now = time.monotonic()

                # Respect Retry-After / preemptive pause
                if now < self._paused_until:
                    wait = self._paused_until - now
                else:
                    # Evict timestamps outside the window
                    cutoff = now - self._window
                    while self._timestamps and self._timestamps[0] <= cutoff:
                        self._timestamps.popleft()

                    if len(self._timestamps) < self._max_calls:
                        # Window has room — record this call and return
                        self._timestamps.append(now)
                        return

                    # Window full — wait until oldest call expires
                    wait = self._timestamps[0] + self._window - now

            # Sleep outside the lock so other threads aren't blocked
            time.sleep(max(wait, 0.001))

    def pause_for(self, seconds: float) -> None:
        """Pause all acquisitions for the given duration (e.g., Retry-After)."""
        with self._lock:
            deadline = time.monotonic() + seconds
            self._paused_until = max(self._paused_until, deadline)
            logger.info("Rate limiter paused for %.1f seconds", seconds)
