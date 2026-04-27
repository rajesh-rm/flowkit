"""Tests for TokenizationClient: HTTP roundtrip, retries, error classes."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest
import respx

from data_assets.extract.tokenization_client import (
    TokenizationClient,
    TokenizationError,
    get_default_client,
    reset_default_client,
)

URL = "https://tokenizer.test/v1/tokenize"


@pytest.fixture
def client():
    c = TokenizationClient(
        base_url=URL, api_key="test-key",
        timeout=2.0, max_attempts=3, base_delay=0.0, max_delay=0.0,
    )
    yield c
    c.close()


# Singleton reset is provided by `_reset_tokenization_singleton` in
# tests/conftest.py (autouse, project-wide).


class TestConstruction:
    def test_requires_url(self):
        with pytest.raises(TokenizationError, match="base_url"):
            TokenizationClient(base_url="", api_key="k")

    def test_requires_key(self):
        with pytest.raises(TokenizationError, match="api_key"):
            TokenizationClient(base_url=URL, api_key="")

    def test_requires_attempts_at_least_one(self):
        with pytest.raises(TokenizationError, match="max_attempts"):
            TokenizationClient(base_url=URL, api_key="k", max_attempts=0)


class TestHappyPath:
    @respx.mock
    def test_returns_tokens_in_input_order(self, client):
        respx.post(URL).mock(
            return_value=httpx.Response(200, json={"tokens": ["t1", "t2", "t3"]}),
        )
        assert client.tokenize(["a", "b", "c"]) == ["t1", "t2", "t3"]

    @respx.mock
    def test_authorization_header_sent(self, client):
        route = respx.post(URL).mock(
            return_value=httpx.Response(200, json={"tokens": ["x"]}),
        )
        client.tokenize(["v"])
        assert route.calls[0].request.headers["authorization"] == "Bearer test-key"

    @respx.mock
    def test_request_body_uses_values_field(self, client):
        route = respx.post(URL).mock(
            return_value=httpx.Response(200, json={"tokens": ["x", "y"]}),
        )
        client.tokenize(["alice", "bob"])
        body = route.calls[0].request.read().decode()
        assert '"values"' in body
        assert "alice" in body
        assert "bob" in body

    def test_empty_input_no_http_call(self, client):
        # Note: no respx.mock — if HTTP were called this would error.
        assert client.tokenize([]) == []

    @respx.mock
    def test_determinism_same_input_same_token(self):
        # Mirrors how the integration test pins endpoint determinism: a
        # f-string transform yields stable tokens across separate calls.
        c = TokenizationClient(URL, "k", base_delay=0.0)
        respx.post(URL).mock(
            side_effect=lambda req: httpx.Response(
                200,
                json={"tokens": [f"tok_{v}" for v in _read_values(req)]},
            ),
        )
        assert c.tokenize(["x"]) == ["tok_x"]
        assert c.tokenize(["x"]) == ["tok_x"]
        c.close()


class TestRetries:
    @respx.mock
    def test_5xx_retried_then_succeeds(self, client):
        responses = [
            httpx.Response(500, text="boom"),
            httpx.Response(503, text="busy"),
            httpx.Response(200, json={"tokens": ["t"]}),
        ]
        respx.post(URL).mock(side_effect=responses)
        assert client.tokenize(["a"]) == ["t"]

    @respx.mock
    def test_5xx_exhausted_raises_tokenization_error(self, client):
        respx.post(URL).mock(return_value=httpx.Response(500, text="dead"))
        with pytest.raises(TokenizationError, match="HTTP 500"):
            client.tokenize(["a"])

    @respx.mock
    def test_timeout_retried_then_raises(self, client):
        respx.post(URL).mock(side_effect=httpx.TimeoutException("slow"))
        with pytest.raises(TokenizationError, match="failed after"):
            client.tokenize(["a"])

    @respx.mock
    def test_network_error_retried_then_raises(self, client):
        respx.post(URL).mock(side_effect=httpx.NetworkError("disconnect"))
        with pytest.raises(TokenizationError, match="failed after"):
            client.tokenize(["a"])


class TestNonRetriableErrors:
    @respx.mock
    def test_4xx_fails_immediately(self, client):
        route = respx.post(URL).mock(
            return_value=httpx.Response(400, text="bad request"),
        )
        with pytest.raises(TokenizationError, match="HTTP 400"):
            client.tokenize(["a"])
        assert route.call_count == 1  # no retry

    @respx.mock
    def test_401_fails_immediately(self, client):
        respx.post(URL).mock(
            return_value=httpx.Response(401, text="unauthorized"),
        )
        with pytest.raises(TokenizationError, match="HTTP 401"):
            client.tokenize(["a"])


class TestResponseValidation:
    @respx.mock
    def test_non_json_response_raises(self, client):
        respx.post(URL).mock(
            return_value=httpx.Response(200, text="not json"),
        )
        with pytest.raises(TokenizationError, match="non-JSON"):
            client.tokenize(["a"])

    @respx.mock
    def test_missing_tokens_field_raises(self, client):
        respx.post(URL).mock(
            return_value=httpx.Response(200, json={"oops": ["x"]}),
        )
        with pytest.raises(TokenizationError, match="missing 'tokens'"):
            client.tokenize(["a"])

    @respx.mock
    def test_tokens_not_a_list_raises(self, client):
        respx.post(URL).mock(
            return_value=httpx.Response(200, json={"tokens": "scalar"}),
        )
        with pytest.raises(TokenizationError, match="not a list"):
            client.tokenize(["a"])

    @respx.mock
    def test_length_mismatch_raises(self, client):
        respx.post(URL).mock(
            return_value=httpx.Response(200, json={"tokens": ["t1"]}),
        )
        with pytest.raises(TokenizationError, match="length mismatch"):
            client.tokenize(["a", "b"])


class TestDefaultClient:

    def test_no_url_raises(self):
        with patch.dict("os.environ", {}, clear=False):
            # ensure both env vars are unset
            with patch.dict(
                "os.environ",
                {"TOKENIZATION_API_URL": "", "TOKENIZATION_API_KEY": "k"},
            ):
                with pytest.raises(TokenizationError, match="TOKENIZATION_API_URL"):
                    get_default_client()

    def test_no_key_raises(self):
        with patch.dict(
            "os.environ",
            {"TOKENIZATION_API_URL": URL, "TOKENIZATION_API_KEY": ""},
        ):
            # The CredentialResolver also checks Airflow connections, but
            # in the test environment Airflow isn't installed, so falls back
            # to the (empty) env var.
            with pytest.raises(TokenizationError, match="TOKENIZATION_API_KEY"):
                get_default_client()

    def test_caches_singleton(self):
        with patch.dict(
            "os.environ",
            {"TOKENIZATION_API_URL": URL, "TOKENIZATION_API_KEY": "k"},
        ):
            c1 = get_default_client()
            c2 = get_default_client()
            assert c1 is c2

    def test_reset_clears_singleton(self):
        with patch.dict(
            "os.environ",
            {"TOKENIZATION_API_URL": URL, "TOKENIZATION_API_KEY": "k"},
        ):
            c1 = get_default_client()
            reset_default_client()
            c2 = get_default_client()
            assert c1 is not c2

    def test_concurrent_first_calls_construct_one_client(self):
        # Regression test for the singleton race: ENTITY_PARALLEL spawns
        # up to 4 worker threads that each call get_default_client(). Two
        # racing threads must NOT each instantiate a TokenizationClient.
        from concurrent.futures import ThreadPoolExecutor
        from data_assets.extract import tokenization_client as tc_mod

        construct_count = 0
        original_init = tc_mod.TokenizationClient.__init__

        def counting_init(self, *args, **kwargs):
            nonlocal construct_count
            construct_count += 1
            original_init(self, *args, **kwargs)

        with patch.dict(
            "os.environ",
            {"TOKENIZATION_API_URL": URL, "TOKENIZATION_API_KEY": "k"},
        ), patch.object(tc_mod.TokenizationClient, "__init__", counting_init):
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(get_default_client) for _ in range(8)]
                clients = [f.result() for f in futures]

        # Exactly one TokenizationClient was constructed across all threads.
        assert construct_count == 1
        # All threads got the same instance back.
        assert all(c is clients[0] for c in clients)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_values(req: httpx.Request) -> list[str]:
    import json
    return json.loads(req.read())["values"]
