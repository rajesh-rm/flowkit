"""HTTP client with rate limiting, token injection, error classification, and retry logic."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from data_assets.core.types import RequestSpec, SkippedRequestError
from data_assets.extract.rate_limiter import RateLimiter
from data_assets.extract.token_manager import TokenManager

logger = logging.getLogger(__name__)

BACKOFF_BASE = 2.0


class APIClient:
    """HTTP client shared across all threads in a parallel extraction.

    Features:
    - Token injection (thread-safe via TokenManager)
    - Rate limiting (thread-safe via RateLimiter)
    - Error classification (retry/skip/fail per asset policy)
    - Rate limit header extraction (preemptive pause)
    - Request stats for run metadata
    """

    def __init__(
        self,
        token_manager: TokenManager,
        rate_limiter: RateLimiter,
        timeout: float = 60.0,
        max_retries: int = 3,
        error_classifier: Any = None,  # callable(status_code, headers) -> str
    ) -> None:
        self._token_manager = token_manager
        self._rate_limiter = rate_limiter
        self._http = httpx.Client(timeout=timeout)
        self._max_retries = max_retries
        self._classify = error_classifier or self._default_classify
        # Stats collected during the client's lifetime
        self._stats = {"api_calls": 0, "retries": 0, "skips": 0, "rate_limit_pauses": 0}

    @property
    def stats(self) -> dict[str, int]:
        """Request statistics for run metadata."""
        return dict(self._stats)

    @staticmethod
    def _default_classify(status_code: int, headers: dict) -> str:
        if status_code == 429 or status_code >= 500:
            return "retry"
        if status_code == 404:
            return "skip"
        return "fail"

    def request(self, spec: RequestSpec) -> Any:
        """Execute an HTTP request with rate limiting, auth, and retries.

        Returns the parsed JSON response body.

        Raises:
            SkippedRequestError: if classify_error returns "skip"
            httpx.HTTPStatusError: if classify_error returns "fail"
        """
        for attempt in range(self._max_retries + 1):
            self._rate_limiter.acquire()
            self._stats["api_calls"] += 1
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
                    self._stats["retries"] += 1
                    wait = BACKOFF_BASE ** (attempt + 1)
                    logger.warning(
                        "Connection error attempt %d/%d: %s. Retrying in %.1fs",
                        attempt + 1, self._max_retries + 1, exc, wait,
                    )
                    time.sleep(wait)
                    continue
                raise

            # Check for errors using classifier
            if response.status_code >= 400:
                action = self._classify(
                    response.status_code, dict(response.headers)
                )

                if action == "skip":
                    self._stats["skips"] += 1
                    logger.warning(
                        "Skipping request: HTTP %d for %s",
                        response.status_code, spec.url,
                    )
                    raise SkippedRequestError(
                        f"HTTP {response.status_code} for {spec.url}"
                    )

                if action == "retry" and attempt < self._max_retries:
                    self._stats["retries"] += 1
                    if response.status_code == 429:
                        retry_after = float(
                            response.headers.get("Retry-After", "30")
                        )
                        self._rate_limiter.pause_for(retry_after)
                        self._stats["rate_limit_pauses"] += 1
                        logger.warning(
                            "HTTP 429 — pausing %.1fs (attempt %d/%d)",
                            retry_after, attempt + 1, self._max_retries + 1,
                        )
                    else:
                        wait = BACKOFF_BASE ** (attempt + 1)
                        logger.warning(
                            "HTTP %d attempt %d/%d. Retrying in %.1fs",
                            response.status_code, attempt + 1,
                            self._max_retries + 1, wait,
                        )
                        time.sleep(wait)
                    continue

                # action == "fail" or retries exhausted
                response.raise_for_status()

            # Success — check rate limit headers for preemptive pause
            self._check_rate_limit_headers(response)

            return response.json()

        raise RuntimeError("Exhausted all retries")

    def _check_rate_limit_headers(self, response: httpx.Response) -> None:
        """If rate limit is nearly exhausted, preemptively pause."""
        remaining = response.headers.get("X-RateLimit-Remaining")
        limit = response.headers.get("X-RateLimit-Limit")
        reset = response.headers.get("X-RateLimit-Reset")

        if remaining is None or limit is None:
            return

        try:
            remaining_int = int(remaining)
            limit_int = int(limit)
        except ValueError:
            return

        # Pause if below 10% of rate limit capacity
        if limit_int > 0 and remaining_int < limit_int * 0.1:
            if reset:
                wait = max(1.0, int(reset) - time.time())
            else:
                wait = 30.0
            self._stats["rate_limit_pauses"] += 1
            logger.info(
                "Rate limit low (%d/%d remaining). Pausing %.0fs.",
                remaining_int, limit_int, wait,
            )
            self._rate_limiter.pause_for(wait)

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()
