"""Tests for APIClient: error classification, rate limit headers, stats."""

from __future__ import annotations

import httpx
import pytest
import respx

from data_assets.core.types import RequestSpec, SkippedRequestError
from data_assets.extract.api_client import APIClient
from data_assets.extract.rate_limiter import RateLimiter
from data_assets.extract.token_manager import TokenManager


class StubTokenManager(TokenManager):
    def get_token(self) -> str:
        return "test-token"

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": "Bearer test-token"}


@pytest.fixture
def client():
    limiter = RateLimiter(rate_per_second=100.0)
    c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=2)
    yield c
    c.close()


# --- Error classification ---

@respx.mock
def test_404_raises_skipped_request_error(client):
    """Default classifier: 404 → skip."""
    respx.get("https://api.test/data").mock(
        return_value=httpx.Response(404, json={"error": "not found"})
    )
    spec = RequestSpec(method="GET", url="https://api.test/data")
    with pytest.raises(SkippedRequestError):
        client.request(spec)
    assert client.stats["skips"] == 1


@respx.mock
def test_custom_classifier_overrides_default():
    """Asset can override classify_error to treat 404 as fail."""
    def strict_classifier(status: int, headers: dict) -> str:
        if status == 404:
            return "fail"
        if status >= 500 or status == 429:
            return "retry"
        return "fail"

    limiter = RateLimiter(rate_per_second=100.0)
    c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=0,
                  error_classifier=strict_classifier)

    respx.get("https://api.test/data").mock(
        return_value=httpx.Response(404, json={"error": "not found"})
    )
    spec = RequestSpec(method="GET", url="https://api.test/data")
    with pytest.raises(httpx.HTTPStatusError):
        c.request(spec)
    c.close()


@respx.mock
def test_403_fails_immediately(client):
    """403 → fail (not retry, not skip)."""
    respx.get("https://api.test/data").mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )
    spec = RequestSpec(method="GET", url="https://api.test/data")
    with pytest.raises(httpx.HTTPStatusError):
        client.request(spec)


# --- Stats tracking ---

@respx.mock
def test_stats_track_api_calls(client):
    respx.get("https://api.test/data").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    spec = RequestSpec(method="GET", url="https://api.test/data")
    client.request(spec)
    client.request(spec)
    assert client.stats["api_calls"] == 2
    assert client.stats["retries"] == 0


@respx.mock
def test_stats_track_retries(client):
    respx.get("https://api.test/data").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    spec = RequestSpec(method="GET", url="https://api.test/data")
    client.request(spec)
    assert client.stats["retries"] == 1
    assert client.stats["api_calls"] == 2  # initial + retry


# --- Rate limit header extraction ---

@respx.mock
def test_rate_limit_headers_trigger_pause(client):
    """Low X-RateLimit-Remaining should trigger preemptive pause."""
    respx.get("https://api.test/data").mock(
        return_value=httpx.Response(200, json={"ok": True}, headers={
            "X-RateLimit-Remaining": "5",
            "X-RateLimit-Limit": "100",
            "X-RateLimit-Reset": str(int(__import__("time").time()) + 2),
        })
    )
    spec = RequestSpec(method="GET", url="https://api.test/data")
    client.request(spec)
    assert client.stats["rate_limit_pauses"] == 1


@respx.mock
def test_no_pause_when_remaining_is_high(client):
    """Plenty of rate limit remaining — no pause."""
    respx.get("https://api.test/data").mock(
        return_value=httpx.Response(200, json={"ok": True}, headers={
            "X-RateLimit-Remaining": "95",
            "X-RateLimit-Limit": "100",
        })
    )
    spec = RequestSpec(method="GET", url="https://api.test/data")
    client.request(spec)
    assert client.stats["rate_limit_pauses"] == 0
