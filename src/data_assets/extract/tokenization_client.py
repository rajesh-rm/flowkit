"""HTTP client for the external tokenization service.

POSTs unique values to the tokenization endpoint and returns same-position
tokenized values. Built around httpx with bounded retries on transient
failures (5xx, timeout, network) and immediate failure on 4xx.

Configuration (via existing CredentialResolver) — see the ``ENV_*``
constants below for the env var names; defaults are documented next to
each constant.
"""

from __future__ import annotations

import logging
import os
import threading
import time

import httpx

from data_assets.extract.token_manager import CredentialResolver

logger = logging.getLogger(__name__)

# Environment variable names. Single source of truth — referenced by both
# the lazy default-client builder and the error messages it raises.
ENV_API_URL = "TOKENIZATION_API_URL"
ENV_API_KEY = "TOKENIZATION_API_KEY"
ENV_TIMEOUT = "TOKENIZATION_TIMEOUT_SECONDS"
ENV_MAX_ATTEMPTS = "TOKENIZATION_MAX_ATTEMPTS"

BACKOFF_BASE = 2.0
DEFAULT_TIMEOUT = 30.0
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_BASE_DELAY = 1.0
DEFAULT_MAX_DELAY = 30.0

# Tokenizer behavior knobs sent in the request body. The service uses
# these to shape the response (token format and length). Defaults match
# the live service's standard configuration; override per-instance via
# the ``options=`` constructor argument when a different token shape is
# needed.
DEFAULT_OPTIONS: dict = {"mode": "opaque", "format": "hex", "token_len": 12}


class TokenizationError(RuntimeError):
    """Raised when the tokenization endpoint cannot fulfil a request.

    Causes include exhausted retries on 5xx/timeout, 4xx responses,
    malformed JSON, and length-mismatched responses. Propagates up through
    write_to_temp to abort the run before any DB write.
    """


class TokenizationClient:
    """Threadsafe client for the tokenization service.

    Request/response contract:
        POST {base_url}
        body:    {"values": ["v1", ...], "options": {"mode": ..., "format": ..., "token_len": N}}
        success: 200 with {"tokens": ["t1", ...], ...} — same length and
                 order as the request values. Extra response metadata
                 fields (``algo``, ``namespace``, ``version``,
                 ``pii_type_counts``, ``collisions``) are tolerated and
                 ignored.

    The client does NOT deduplicate — callers send already-deduplicated
    values to keep payloads minimal. The client only verifies the response
    is well-formed and the same length as the input.

    Authentication is optional. When ``api_key`` is provided, an
    ``Authorization: Bearer ...`` header is added; when omitted, the
    client makes unauthenticated calls (the live service accepts these).
    """

    def __init__(
        self,
        base_url: str,
        api_key: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_delay: float = DEFAULT_BASE_DELAY,
        max_delay: float = DEFAULT_MAX_DELAY,
        options: dict | None = None,
    ) -> None:
        if not base_url:
            raise TokenizationError("TokenizationClient requires base_url")
        if max_attempts < 1:
            raise TokenizationError("max_attempts must be >= 1")
        self._url = base_url
        self._options = dict(options) if options is not None else dict(DEFAULT_OPTIONS)
        headers = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._http = httpx.Client(timeout=timeout, headers=headers)
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay

    def tokenize(self, values: list[str]) -> list[str]:
        """Send `values` (already deduplicated) and return positional tokens.

        Empty input is a no-op and returns an empty list without an HTTP
        call. Bounded retry on transient failures; raises
        ``TokenizationError`` on 4xx, exhausted retries, malformed JSON, or
        length mismatch.
        """
        if not values:
            return []

        body = {"values": values, "options": self._options}
        last_exc: Exception | None = None
        for attempt in range(1, self._max_attempts + 1):
            try:
                response = self._http.post(self._url, json=body)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_exc = exc
                if attempt >= self._max_attempts:
                    raise TokenizationError(
                        f"Tokenization request failed after {attempt} attempts: {exc}"
                    ) from exc
                self._sleep_for_retry(attempt, reason=str(exc))
                continue

            if response.status_code >= 500:
                last_exc = httpx.HTTPStatusError(
                    f"HTTP {response.status_code}",
                    request=response.request,
                    response=response,
                )
                if attempt >= self._max_attempts:
                    raise TokenizationError(
                        f"Tokenization endpoint returned "
                        f"HTTP {response.status_code} after {attempt} attempts"
                    ) from last_exc
                self._sleep_for_retry(
                    attempt, reason=f"HTTP {response.status_code}",
                )
                continue

            if response.status_code >= 400:
                raise TokenizationError(
                    f"Tokenization endpoint returned "
                    f"HTTP {response.status_code}: "
                    f"{response.text[:200]}"
                )

            return self._parse_and_validate(response, expected_len=len(values))

        # Should be unreachable — retry loop always either returns or raises.
        raise TokenizationError(
            f"Tokenization request failed: {last_exc}"
        ) from last_exc

    def _parse_and_validate(
        self, response: httpx.Response, expected_len: int,
    ) -> list[str]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise TokenizationError(
                f"Tokenization endpoint returned non-JSON: "
                f"{response.text[:200]}"
            ) from exc

        if not isinstance(payload, dict) or "tokens" not in payload:
            raise TokenizationError(
                f"Tokenization response missing 'tokens' field: "
                f"{str(payload)[:200]}"
            )

        tokens = payload["tokens"]
        if not isinstance(tokens, list):
            raise TokenizationError(
                f"Tokenization 'tokens' field is not a list: {type(tokens).__name__}"
            )

        if len(tokens) != expected_len:
            raise TokenizationError(
                f"Tokenization response length mismatch: "
                f"sent {expected_len}, received {len(tokens)}"
            )

        return [str(t) for t in tokens]

    def _sleep_for_retry(self, attempt: int, reason: str) -> None:
        wait = min(self._base_delay * (BACKOFF_BASE ** (attempt - 1)), self._max_delay)
        logger.warning(
            "Tokenization attempt %d/%d failed (%s). Retrying in %.1fs.",
            attempt, self._max_attempts, reason, wait,
        )
        time.sleep(wait)

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._http.close()


_resolver = CredentialResolver()
_default_client: TokenizationClient | None = None
_default_client_lock = threading.Lock()


def get_default_client() -> TokenizationClient:
    """Return a process-wide TokenizationClient, building it on first use.

    Reads ``TOKENIZATION_API_URL`` (required) and ``TOKENIZATION_API_KEY``
    (optional — the live service accepts unauthenticated calls) from the
    environment, or Airflow connection ``tokenization_api`` for the key.
    Raises ``TokenizationError`` only when the URL is missing — deferred
    to first call so test suites and asset definitions that never use
    sensitive columns can run without any configuration.

    Thread-safe via double-checked locking: ENTITY_PARALLEL extraction
    spawns multiple worker threads that each may race on the first call.
    Without the lock, two threads could both pass the None check and each
    instantiate an httpx.Client; the loser's connection pool would leak.
    """
    if _default_client is not None:
        return _default_client

    with _default_client_lock:
        # Re-check inside the lock — another thread may have built the
        # client while we were waiting.
        if _default_client is not None:
            return _default_client
        return _build_default_client()


def _build_default_client() -> TokenizationClient:
    """Build the singleton from environment. Caller must hold the lock."""
    global _default_client

    base_url = os.environ.get(ENV_API_URL)
    if not base_url:
        raise TokenizationError(
            f"{ENV_API_URL} is not set. Required for assets with "
            f"contains_sensitive_data=True."
        )

    # Optional — the live service accepts unauthenticated calls. When
    # set (env var or Airflow connection 'tokenization_api'), the
    # Authorization header is added; when missing, the request is sent
    # without auth.
    api_key = _resolver.resolve(ENV_API_KEY)

    timeout_raw = os.environ.get(ENV_TIMEOUT)
    timeout = float(timeout_raw) if timeout_raw else DEFAULT_TIMEOUT

    attempts_raw = os.environ.get(ENV_MAX_ATTEMPTS)
    max_attempts = int(attempts_raw) if attempts_raw else DEFAULT_MAX_ATTEMPTS

    _default_client = TokenizationClient(
        base_url=base_url,
        api_key=api_key,
        timeout=timeout,
        max_attempts=max_attempts,
    )
    return _default_client


def reset_default_client() -> None:
    """Tear down the cached singleton — primarily for tests.

    Early-returns when the client is already None so the autouse test
    fixture (which fires twice per test) doesn't pay an avoidable lock
    acquisition on the dominant path. The post-acquire re-check still
    guards correctness if two callers race on the close.
    """
    global _default_client
    if _default_client is None:
        return
    with _default_client_lock:
        if _default_client is not None:
            _default_client.close()
            _default_client = None
