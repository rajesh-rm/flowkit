"""Tests for the API client with mocked HTTP responses."""

from __future__ import annotations

import httpx
import pytest
import respx

from data_assets.core.types import RequestSpec
from data_assets.extract.api_client import APIClient
from data_assets.extract.rate_limiter import RateLimiter
from data_assets.extract.token_manager import TokenManager


class StubTokenManager(TokenManager):
    """Minimal token manager for testing."""

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


@respx.mock
def test_successful_request(client):
    respx.get("https://api.example.com/data").mock(
        return_value=httpx.Response(200, json={"items": [1, 2, 3]})
    )
    spec = RequestSpec(method="GET", url="https://api.example.com/data")
    result = client.request(spec)
    assert result == {"items": [1, 2, 3]}


@respx.mock
def test_auth_header_injected(client):
    route = respx.get("https://api.example.com/data").mock(
        return_value=httpx.Response(200, json={})
    )
    spec = RequestSpec(method="GET", url="https://api.example.com/data")
    client.request(spec)
    assert route.calls[0].request.headers["authorization"] == "Bearer test-token"


@respx.mock
def test_retry_on_500(client):
    route = respx.get("https://api.example.com/data").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    spec = RequestSpec(method="GET", url="https://api.example.com/data")
    result = client.request(spec)
    assert result == {"ok": True}
    assert route.call_count == 2


@respx.mock
def test_fail_on_4xx(client):
    respx.get("https://api.example.com/data").mock(
        return_value=httpx.Response(403, json={"error": "forbidden"})
    )
    spec = RequestSpec(method="GET", url="https://api.example.com/data")
    with pytest.raises(httpx.HTTPStatusError):
        client.request(spec)


@respx.mock
def test_retry_on_429(client):
    route = respx.get("https://api.example.com/data").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "0.1"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    spec = RequestSpec(method="GET", url="https://api.example.com/data")
    result = client.request(spec)
    assert result == {"ok": True}
    assert route.call_count == 2


@respx.mock
def test_post_with_body(client):
    route = respx.post("https://api.example.com/data").mock(
        return_value=httpx.Response(200, json={"created": True})
    )
    spec = RequestSpec(
        method="POST",
        url="https://api.example.com/data",
        body={"name": "test"},
    )
    result = client.request(spec)
    assert result["created"] is True
