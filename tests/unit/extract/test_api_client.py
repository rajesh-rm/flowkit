"""Tests for APIClient: HTTP requests, retries, error classification, rate limits."""

from __future__ import annotations

import httpx
import pytest
import respx

from data_assets.core.types import RequestSpec, SkippedRequestError
from data_assets.extract.api_client import APIClient
from data_assets.extract.rate_limiter import RateLimiter
from tests.conftest import StubTokenManager

SPEC = RequestSpec(method="GET", url="https://api.test/data")


@pytest.fixture
def client():
    limiter = RateLimiter(rate_per_second=100.0)
    c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=2)
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestSuccessfulRequests:
    @respx.mock
    def test_get_returns_json(self, client):
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={"items": [1, 2, 3]})
        )
        assert client.request(SPEC) == {"items": [1, 2, 3]}

    @respx.mock
    def test_post_with_body(self, client):
        respx.post("https://api.test/data").mock(
            return_value=httpx.Response(200, json={"created": True})
        )
        spec = RequestSpec(method="POST", url="https://api.test/data", body={"name": "x"})
        assert client.request(spec)["created"] is True

    @respx.mock
    def test_auth_header_injected(self, client):
        route = respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={})
        )
        client.request(SPEC)
        assert route.calls[0].request.headers["authorization"] == "Bearer test-token"


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------


class TestErrorClassification:
    @respx.mock
    def test_404_raises_skipped_request_error(self, client):
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(404, json={"error": "not found"})
        )
        with pytest.raises(SkippedRequestError):
            client.request(SPEC)
        assert client.stats["skips"] == 1

    @respx.mock
    def test_403_fails_immediately(self, client):
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(403, json={"error": "forbidden"})
        )
        with pytest.raises(httpx.HTTPStatusError):
            client.request(SPEC)

    @respx.mock
    def test_custom_classifier_overrides_default(self):
        """Asset can override classify_error to treat 404 as fail."""
        def strict(status, headers):
            return "fail"

        limiter = RateLimiter(rate_per_second=100.0)
        c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=0,
                      error_classifier=strict)
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(404)
        )
        with pytest.raises(httpx.HTTPStatusError):
            c.request(SPEC)
        c.close()


# ---------------------------------------------------------------------------
# Retry behavior
# ---------------------------------------------------------------------------


class TestRetryBehavior:
    @respx.mock
    def test_retry_on_500(self, client):
        route = respx.get("https://api.test/data").mock(
            side_effect=[httpx.Response(500), httpx.Response(200, json={"ok": True})]
        )
        assert client.request(SPEC) == {"ok": True}
        assert route.call_count == 2

    @respx.mock
    def test_retry_on_429_with_retry_after(self, client):
        route = respx.get("https://api.test/data").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0.1"}),
                httpx.Response(200, json={"ok": True}),
            ]
        )
        assert client.request(SPEC) == {"ok": True}
        assert route.call_count == 2

    @respx.mock
    def test_retry_on_429_with_date_retry_after(self, client):
        """Retry-After as HTTP date string should not crash (falls back to 30s default)."""
        route = respx.get("https://api.test/data").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "Thu, 01 Dec 2025 16:00:00 GMT"}),
                httpx.Response(200, json={"ok": True}),
            ]
        )
        assert client.request(SPEC) == {"ok": True}
        assert route.call_count == 2

    @respx.mock
    def test_retries_exhausted_raises(self):
        """After max_retries, the error should propagate."""
        limiter = RateLimiter(rate_per_second=100.0)
        c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=1)
        respx.get("https://api.test/data").mock(
            side_effect=[httpx.Response(500), httpx.Response(500)]
        )
        with pytest.raises(httpx.HTTPStatusError):
            c.request(SPEC)
        assert c.stats["retries"] == 1
        assert c.stats["api_calls"] == 0  # no successful calls
        c.close()

    @respx.mock
    def test_connection_error_retries_then_raises(self):
        """Connection errors should retry, then propagate on exhaustion."""
        limiter = RateLimiter(rate_per_second=100.0)
        c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=1)
        respx.get("https://api.test/data").mock(
            side_effect=httpx.ConnectError("refused")
        )
        with pytest.raises(httpx.ConnectError):
            c.request(SPEC)
        assert c.stats["retries"] == 1
        c.close()

    @respx.mock
    def test_connection_error_recovers(self):
        """Connection error on first try, success on second."""
        limiter = RateLimiter(rate_per_second=100.0)
        c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=2)
        respx.get("https://api.test/data").mock(
            side_effect=[httpx.ConnectError("refused"), httpx.Response(200, json={"ok": True})]
        )
        assert c.request(SPEC) == {"ok": True}
        assert c.stats["retries"] == 1
        c.close()


# ---------------------------------------------------------------------------
# Stats tracking
# ---------------------------------------------------------------------------


class TestStatsTracking:
    @respx.mock
    def test_api_calls_counted(self, client):
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={})
        )
        client.request(SPEC)
        client.request(SPEC)
        assert client.stats["api_calls"] == 2

    @respx.mock
    def test_retries_counted(self, client):
        respx.get("https://api.test/data").mock(
            side_effect=[httpx.Response(500), httpx.Response(200, json={})]
        )
        client.request(SPEC)
        assert client.stats["retries"] == 1
        assert client.stats["api_calls"] == 1  # only the successful request


# ---------------------------------------------------------------------------
# Rate limit header handling
# ---------------------------------------------------------------------------


class TestRateLimitHeaders:
    @respx.mock
    def test_low_remaining_triggers_pause(self, client):
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={}, headers={
                "X-RateLimit-Remaining": "5",
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Reset": str(int(__import__("time").time()) + 2),
            })
        )
        client.request(SPEC)
        assert client.stats["rate_limit_pauses"] == 1

    @respx.mock
    def test_high_remaining_no_pause(self, client):
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={}, headers={
                "X-RateLimit-Remaining": "95",
                "X-RateLimit-Limit": "100",
            })
        )
        client.request(SPEC)
        assert client.stats["rate_limit_pauses"] == 0

    @respx.mock
    def test_missing_headers_no_pause(self, client):
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={})
        )
        client.request(SPEC)
        assert client.stats["rate_limit_pauses"] == 0

    @respx.mock
    def test_non_numeric_reset_header_uses_default(self, client):
        """Non-numeric X-RateLimit-Reset should not crash (falls back to 30s)."""
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={}, headers={
                "X-RateLimit-Remaining": "2",
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Reset": "2025-12-01T16:00:00Z",
            })
        )
        client.request(SPEC)
        assert client.stats["rate_limit_pauses"] == 1


# ---------------------------------------------------------------------------
# Timeout handling
# ---------------------------------------------------------------------------


class TestTimeoutHandling:
    @respx.mock
    def test_timeout_retries_then_raises(self):
        """httpx.TimeoutException should retry, then propagate on exhaustion."""
        limiter = RateLimiter(rate_per_second=100.0)
        c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=1)
        respx.get("https://api.test/data").mock(
            side_effect=httpx.ReadTimeout("read timed out")
        )
        with pytest.raises(httpx.ReadTimeout):
            c.request(SPEC)
        assert c.stats["retries"] == 1
        c.close()

    @respx.mock
    def test_timeout_recovers_on_retry(self):
        """Timeout on first try, success on second."""
        limiter = RateLimiter(rate_per_second=100.0)
        c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=2)
        respx.get("https://api.test/data").mock(
            side_effect=[
                httpx.ReadTimeout("read timed out"),
                httpx.Response(200, json={"recovered": True}),
            ]
        )
        assert c.request(SPEC) == {"recovered": True}
        assert c.stats["retries"] == 1
        c.close()


# ---------------------------------------------------------------------------
# Rate limit exhaustion (429 all retries)
# ---------------------------------------------------------------------------


class TestRateLimitExhaustion:
    @respx.mock
    def test_429_exhausts_all_retries(self):
        """Persistent 429 should exhaust retries and raise."""
        limiter = RateLimiter(rate_per_second=100.0)
        c = APIClient(StubTokenManager(), limiter, timeout=5.0, max_retries=1)
        respx.get("https://api.test/data").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0.01"}),
                httpx.Response(429, headers={"Retry-After": "0.01"}),
            ]
        )
        with pytest.raises(httpx.HTTPStatusError):
            c.request(SPEC)
        assert c.stats["retries"] >= 1
        c.close()

    @respx.mock
    def test_zero_remaining_triggers_preemptive_pause(self, client):
        """X-RateLimit-Remaining: 0 should trigger preemptive pause."""
        respx.get("https://api.test/data").mock(
            return_value=httpx.Response(200, json={}, headers={
                "X-RateLimit-Remaining": "0",
                "X-RateLimit-Limit": "100",
                "X-RateLimit-Reset": str(int(__import__("time").time()) + 1),
            })
        )
        client.request(SPEC)
        assert client.stats["rate_limit_pauses"] == 1
