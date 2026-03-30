"""HTTP client that ties together token management, rate limiting, and request execution."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from data_assets.core.types import RequestSpec
from data_assets.extract.rate_limiter import RateLimiter
from data_assets.extract.token_manager import TokenManager

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60.0
MAX_RETRIES = 3
BACKOFF_BASE = 2.0


class APIClient:
    """HTTP client with rate limiting, token injection, and retry logic.

    One instance is shared across all threads in a parallel extraction.
    The rate limiter and token manager are thread-safe.
    """

    def __init__(
        self,
        token_manager: TokenManager,
        rate_limiter: RateLimiter,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self._token_manager = token_manager
        self._rate_limiter = rate_limiter
        self._http = httpx.Client(timeout=timeout)
        self._max_retries = max_retries

    def request(self, spec: RequestSpec) -> Any:
        """Execute an HTTP request with rate limiting, auth, and retries.

        Returns the parsed JSON response body.

        Retry logic:
        - HTTP 429: pause for Retry-After, then retry
        - HTTP 5xx: exponential backoff
        - HTTP 4xx (not 429): fail immediately
        - Connection/timeout errors: exponential backoff
        """
        for attempt in range(self._max_retries + 1):
            self._rate_limiter.acquire()
            auth_headers = self._token_manager.get_auth_header()

            merged_headers = dict(spec.headers or {})
            merged_headers.update(auth_headers)

            try:
                response = self._http.request(
                    method=spec.method,
                    url=spec.url,
                    params=spec.params,
                    headers=merged_headers,
                    json=spec.body,
                )
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                if attempt < self._max_retries:
                    wait = BACKOFF_BASE ** (attempt + 1)
                    logger.warning(
                        "Connection error on attempt %d/%d: %s. Retrying in %.1fs",
                        attempt + 1,
                        self._max_retries + 1,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                raise

            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", "30"))
                self._rate_limiter.pause_for(retry_after)
                if attempt < self._max_retries:
                    logger.warning(
                        "HTTP 429 — pausing %.1fs (attempt %d/%d)",
                        retry_after,
                        attempt + 1,
                        self._max_retries + 1,
                    )
                    continue
                response.raise_for_status()

            if 500 <= response.status_code < 600:
                if attempt < self._max_retries:
                    wait = BACKOFF_BASE ** (attempt + 1)
                    logger.warning(
                        "HTTP %d on attempt %d/%d. Retrying in %.1fs",
                        response.status_code,
                        attempt + 1,
                        self._max_retries + 1,
                        wait,
                    )
                    time.sleep(wait)
                    continue
                response.raise_for_status()

            # 4xx (not 429) — fail immediately
            response.raise_for_status()

            return response.json()

        # Should not reach here, but just in case
        raise RuntimeError("Exhausted all retries")

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()
