"""Tests for token manager credential validation at initialization."""

from __future__ import annotations

import pytest

from data_assets.extract.token_manager import (
    GitHubAppTokenManager,
    JiraTokenManager,
    ServiceNowTokenManager,
    SonarQubeTokenManager,
)


class TestGitHubAppTokenManager:
    def test_missing_app_id_raises(self, monkeypatch):
        monkeypatch.delenv("GITHUB_APP_ID", raising=False)
        monkeypatch.setenv("GITHUB_PRIVATE_KEY", "key")
        monkeypatch.setenv("GITHUB_INSTALLATION_ID", "123")
        with pytest.raises(RuntimeError, match="GITHUB_APP_ID"):
            GitHubAppTokenManager()

    def test_missing_private_key_raises(self, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "1")
        monkeypatch.delenv("GITHUB_PRIVATE_KEY", raising=False)
        monkeypatch.setenv("GITHUB_INSTALLATION_ID", "123")
        with pytest.raises(RuntimeError, match="GITHUB_PRIVATE_KEY"):
            GitHubAppTokenManager()

    def test_missing_installation_id_raises(self, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "1")
        monkeypatch.setenv("GITHUB_PRIVATE_KEY", "key")
        monkeypatch.delenv("GITHUB_INSTALLATION_ID", raising=False)
        with pytest.raises(RuntimeError, match="GITHUB_INSTALLATION_ID"):
            GitHubAppTokenManager()


class TestServiceNowTokenManager:
    def test_missing_instance_raises(self, monkeypatch):
        monkeypatch.delenv("SERVICENOW_INSTANCE", raising=False)
        with pytest.raises(RuntimeError, match="SERVICENOW_INSTANCE"):
            ServiceNowTokenManager()

    def test_missing_both_auth_methods_raises(self, monkeypatch):
        monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
        monkeypatch.delenv("SERVICENOW_CLIENT_ID", raising=False)
        monkeypatch.delenv("SERVICENOW_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("SERVICENOW_USERNAME", raising=False)
        monkeypatch.delenv("SERVICENOW_PASSWORD", raising=False)
        with pytest.raises(RuntimeError, match="SERVICENOW_CLIENT_ID"):
            ServiceNowTokenManager()


class TestSonarQubeTokenManager:
    def test_missing_token_raises(self, monkeypatch):
        monkeypatch.delenv("SONARQUBE_TOKEN", raising=False)
        with pytest.raises(RuntimeError, match="SONARQUBE_TOKEN"):
            SonarQubeTokenManager()


class TestJiraTokenManager:
    def test_missing_all_credentials_raises(self, monkeypatch):
        monkeypatch.delenv("JIRA_PAT", raising=False)
        monkeypatch.delenv("JIRA_EMAIL", raising=False)
        monkeypatch.delenv("JIRA_API_TOKEN", raising=False)
        with pytest.raises(RuntimeError, match="JIRA_PAT"):
            JiraTokenManager()

    def test_missing_email_with_token_raises(self, monkeypatch):
        monkeypatch.delenv("JIRA_PAT", raising=False)
        monkeypatch.delenv("JIRA_EMAIL", raising=False)
        monkeypatch.setenv("JIRA_API_TOKEN", "tok")
        with pytest.raises(RuntimeError):
            JiraTokenManager()


# ---------------------------------------------------------------------------
# CredentialResolver — Airflow paths
# ---------------------------------------------------------------------------


class TestCredentialResolver:
    def test_from_airflow_returns_password(self, monkeypatch):
        """_from_airflow returns conn.password when Airflow is available."""
        from unittest.mock import MagicMock
        from data_assets.extract.token_manager import CredentialResolver

        resolver = CredentialResolver()

        fake_conn = MagicMock()
        fake_conn.password = "airflow-secret"
        fake_conn.get_uri.return_value = "postgres://..."

        fake_base_hook = MagicMock()
        fake_base_hook.get_connection.return_value = fake_conn

        import sys
        fake_module = MagicMock()
        fake_module.BaseHook = fake_base_hook
        monkeypatch.setitem(sys.modules, "airflow.sdk", fake_module)

        # Ensure env var doesn't shadow the test
        monkeypatch.delenv("MY_AIRFLOW_KEY", raising=False)

        result = resolver.resolve("MY_AIRFLOW_KEY")
        assert result == "airflow-secret"
        fake_base_hook.get_connection.assert_called_once_with("MY_AIRFLOW_KEY")

    def test_from_airflow_import_error_falls_through(self, monkeypatch):
        """ImportError in _from_airflow returns None (falls through to env)."""
        from data_assets.extract.token_manager import CredentialResolver

        resolver = CredentialResolver()

        # Make sure airflow is NOT importable by removing any cached module
        import sys
        monkeypatch.delitem(sys.modules, "airflow.sdk", raising=False)
        monkeypatch.delitem(sys.modules, "airflow", raising=False)

        monkeypatch.setenv("SOME_KEY", "from-env")
        assert resolver.resolve("SOME_KEY") == "from-env"

    def test_from_airflow_exception_returns_none(self, monkeypatch):
        """Non-import exception in _from_airflow logs warning and returns None."""
        from unittest.mock import MagicMock
        from data_assets.extract.token_manager import CredentialResolver

        resolver = CredentialResolver()

        fake_base_hook = MagicMock()
        fake_base_hook.get_connection.side_effect = Exception("connection not found")

        import sys
        fake_module = MagicMock()
        fake_module.BaseHook = fake_base_hook
        monkeypatch.setitem(sys.modules, "airflow.sdk", fake_module)

        monkeypatch.setenv("FAIL_KEY", "env-fallback")
        assert resolver.resolve("FAIL_KEY") == "env-fallback"


# ---------------------------------------------------------------------------
# GitHubAppTokenManager — get_token / get_auth_header
# ---------------------------------------------------------------------------


class TestGitHubAppTokenManagerGetToken:
    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("GITHUB_APP_ID", "12345")
        monkeypatch.setenv("GITHUB_PRIVATE_KEY", "fake-key")
        monkeypatch.setenv("GITHUB_INSTALLATION_ID", "67890")

    def test_get_token_returns_cached(self, monkeypatch):
        """get_token returns cached token when not near expiry."""
        from unittest.mock import patch
        import time

        mgr = GitHubAppTokenManager()
        # Manually seed a cached token
        mgr._token = "cached-gh-token"
        mgr._expires_at = time.time() + 3600  # 1 hour from now

        # _refresh should NOT be called
        with patch.object(mgr, "_refresh") as mock_refresh:
            result = mgr.get_token()
            mock_refresh.assert_not_called()
        assert result == "cached-gh-token"

    def test_get_token_refreshes_when_expired(self, monkeypatch):
        """get_token calls _refresh when token is expired / near expiry."""
        from unittest.mock import patch, MagicMock
        import time

        mgr = GitHubAppTokenManager()
        mgr._token = "old-token"
        mgr._expires_at = time.time() + 100  # within REFRESH_MARGIN (300)

        def fake_refresh():
            mgr._token = "fresh-gh-token"
            mgr._expires_at = time.time() + 3600

        with patch.object(mgr, "_refresh", side_effect=fake_refresh):
            result = mgr.get_token()
        assert result == "fresh-gh-token"

    def test_get_token_refresh_via_http(self, monkeypatch):
        """get_token performs JWT + HTTP refresh on first call."""
        from unittest.mock import patch, MagicMock
        import httpx
        import jwt as jwt_mod

        mgr = GitHubAppTokenManager()

        mock_response = MagicMock()
        mock_response.json.return_value = {"token": "inst-token-abc"}
        mock_response.raise_for_status = MagicMock()

        with patch.object(jwt_mod, "encode", return_value="fake-jwt"), \
             patch.object(httpx, "post", return_value=mock_response):
            token = mgr.get_token()

        assert token == "inst-token-abc"

    def test_get_auth_header(self, monkeypatch):
        """get_auth_header returns Bearer header with current token."""
        from unittest.mock import patch
        import time

        mgr = GitHubAppTokenManager()
        mgr._token = "gh-tok-123"
        mgr._expires_at = time.time() + 3600

        header = mgr.get_auth_header()
        assert header == {"Authorization": "Bearer gh-tok-123"}


# ---------------------------------------------------------------------------
# ServiceNowTokenManager — basic auth & OAuth paths
# ---------------------------------------------------------------------------


class TestServiceNowBasicAuth:
    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
        monkeypatch.delenv("SERVICENOW_CLIENT_ID", raising=False)
        monkeypatch.delenv("SERVICENOW_CLIENT_SECRET", raising=False)
        monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
        monkeypatch.setenv("SERVICENOW_PASSWORD", "s3cret")

    def test_get_token_returns_password(self):
        mgr = ServiceNowTokenManager()
        assert mgr.get_token() == "s3cret"

    def test_get_auth_header_basic(self):
        import base64

        mgr = ServiceNowTokenManager()
        header = mgr.get_auth_header()
        expected = base64.b64encode(b"admin:s3cret").decode()
        assert header == {"Authorization": f"Basic {expected}"}


class TestServiceNowOAuth:
    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
        monkeypatch.setenv("SERVICENOW_CLIENT_ID", "client-abc")
        monkeypatch.setenv("SERVICENOW_CLIENT_SECRET", "client-secret")
        monkeypatch.delenv("SERVICENOW_USERNAME", raising=False)
        monkeypatch.delenv("SERVICENOW_PASSWORD", raising=False)

    def test_get_token_oauth_refresh(self):
        """First call triggers _refresh via httpx, returns access_token."""
        from unittest.mock import patch, MagicMock
        import httpx

        mgr = ServiceNowTokenManager()

        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "access_token": "snow-oauth-token",
            "expires_in": 1800,
        }
        mock_resp.raise_for_status = MagicMock()

        with patch.object(httpx, "post", return_value=mock_resp) as mock_post:
            token = mgr.get_token()

        assert token == "snow-oauth-token"
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert "oauth_token.do" in call_kwargs[0][0]

    def test_get_token_returns_cached(self):
        """Cached OAuth token is returned without refresh."""
        from unittest.mock import patch
        import time

        mgr = ServiceNowTokenManager()
        mgr._token = "cached-snow-token"
        mgr._expires_at = time.time() + 1800

        with patch.object(mgr, "_refresh") as mock_refresh:
            token = mgr.get_token()
            mock_refresh.assert_not_called()
        assert token == "cached-snow-token"

    def test_get_auth_header_oauth(self):
        """OAuth mode returns Bearer header."""
        import time

        mgr = ServiceNowTokenManager()
        mgr._token = "snow-bearer"
        mgr._expires_at = time.time() + 1800

        header = mgr.get_auth_header()
        assert header == {"Authorization": "Bearer snow-bearer"}


# ---------------------------------------------------------------------------
# SonarQubeTokenManager — get_token / get_auth_header
# ---------------------------------------------------------------------------


class TestSonarQubeTokenManagerGetToken:
    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("SONARQUBE_TOKEN", "sqt-abc-123")

    def test_get_token(self):
        mgr = SonarQubeTokenManager()
        assert mgr.get_token() == "sqt-abc-123"

    def test_get_auth_header(self):
        mgr = SonarQubeTokenManager()
        assert mgr.get_auth_header() == {"Authorization": "Bearer sqt-abc-123"}


# ---------------------------------------------------------------------------
# JiraTokenManager — PAT and Cloud modes
# ---------------------------------------------------------------------------


class TestJiraTokenManagerPAT:
    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.setenv("JIRA_PAT", "jira-pat-token")
        monkeypatch.delenv("JIRA_EMAIL", raising=False)
        monkeypatch.delenv("JIRA_API_TOKEN", raising=False)

    def test_get_token_returns_pat(self):
        mgr = JiraTokenManager()
        assert mgr.get_token() == "jira-pat-token"

    def test_get_auth_header_bearer(self):
        mgr = JiraTokenManager()
        assert mgr.get_auth_header() == {"Authorization": "Bearer jira-pat-token"}


class TestJiraTokenManagerCloud:
    @pytest.fixture(autouse=True)
    def _setup_env(self, monkeypatch):
        monkeypatch.delenv("JIRA_PAT", raising=False)
        monkeypatch.setenv("JIRA_EMAIL", "user@example.com")
        monkeypatch.setenv("JIRA_API_TOKEN", "jira-api-tok")

    def test_get_token_returns_api_token(self):
        mgr = JiraTokenManager()
        assert mgr.get_token() == "jira-api-tok"

    def test_get_auth_header_basic(self):
        import base64

        mgr = JiraTokenManager()
        header = mgr.get_auth_header()
        expected = base64.b64encode(b"user@example.com:jira-api-tok").decode()
        assert header == {"Authorization": f"Basic {expected}"}
