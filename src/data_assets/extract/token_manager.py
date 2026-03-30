"""Pluggable token managers for API credential lifecycle.

Each source has its own TokenManager subclass that handles credential
resolution, caching, and refresh (including mid-run rotation).
"""

from __future__ import annotations

import logging
import os
import threading
import time
from abc import ABC, abstractmethod

from dotenv import load_dotenv

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Credential resolver
# ---------------------------------------------------------------------------

class CredentialResolver:
    """Resolves initial secrets from Airflow Connections, env vars, or .env."""

    def resolve(self, key: str) -> str | None:
        """Return the secret value for the given key, or None."""
        # 1. Airflow Connection
        val = self._from_airflow(key)
        if val:
            return val

        # 2. Environment variable
        val = os.environ.get(key)
        if val:
            return val

        # 3. .env file
        load_dotenv()
        return os.environ.get(key)

    @staticmethod
    def _from_airflow(key: str) -> str | None:
        try:
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(key)
            return conn.password or conn.get_uri()
        except Exception:
            return None


_resolver = CredentialResolver()


# ---------------------------------------------------------------------------
# Base TokenManager
# ---------------------------------------------------------------------------

class TokenManager(ABC):
    """Base class for all token managers.

    Thread-safe: get_token() and get_auth_header() may be called from
    multiple extraction threads concurrently.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()

    @abstractmethod
    def get_token(self) -> str:
        """Return a valid token/credential string."""
        ...

    @abstractmethod
    def get_auth_header(self) -> dict[str, str]:
        """Return HTTP header(s) for authentication."""
        ...


# ---------------------------------------------------------------------------
# GitHub App Token Manager
# ---------------------------------------------------------------------------

class GitHubAppTokenManager(TokenManager):
    """Generates GitHub App installation tokens (1-hour validity).

    Proactively refreshes when token is within 5 minutes of expiry.
    Requires: GITHUB_APP_ID, GITHUB_PRIVATE_KEY, GITHUB_INSTALLATION_ID.
    """

    REFRESH_MARGIN = 300  # seconds before expiry to refresh

    def __init__(self) -> None:
        super().__init__()
        self._app_id = _resolver.resolve("GITHUB_APP_ID")
        self._private_key = _resolver.resolve("GITHUB_PRIVATE_KEY")
        self._installation_id = _resolver.resolve("GITHUB_INSTALLATION_ID")
        self._token: str | None = None
        self._expires_at: float = 0.0

        if not all([self._app_id, self._private_key, self._installation_id]):
            raise RuntimeError(
                "GitHubAppTokenManager requires GITHUB_APP_ID, GITHUB_PRIVATE_KEY, "
                "and GITHUB_INSTALLATION_ID"
            )

    def get_token(self) -> str:
        with self._lock:
            if self._token and time.time() < (self._expires_at - self.REFRESH_MARGIN):
                return self._token
            self._refresh()
            return self._token  # type: ignore[return-value]

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.get_token()}"}

    def _refresh(self) -> None:
        import httpx
        import jwt

        now = int(time.time())
        payload = {
            "iat": now - 60,
            "exp": now + 600,
            "iss": self._app_id,
        }
        encoded_jwt = jwt.encode(payload, self._private_key, algorithm="RS256")

        resp = httpx.post(
            f"https://api.github.com/app/installations/{self._installation_id}"
            "/access_tokens",
            headers={
                "Authorization": f"Bearer {encoded_jwt}",
                "Accept": "application/vnd.github+json",
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["token"]
        self._expires_at = time.time() + 3600  # 1 hour
        logger.debug("Refreshed GitHub App token (installation %s)", self._installation_id)


# ---------------------------------------------------------------------------
# ServiceNow Token Manager
# ---------------------------------------------------------------------------

class ServiceNowTokenManager(TokenManager):
    """OAuth2 client_credentials flow, with basic auth fallback.

    Requires: SERVICENOW_INSTANCE, SERVICENOW_CLIENT_ID, SERVICENOW_CLIENT_SECRET
    OR: SERVICENOW_INSTANCE, SERVICENOW_USERNAME, SERVICENOW_PASSWORD (basic auth)
    """

    def __init__(self) -> None:
        super().__init__()
        self._instance = _resolver.resolve("SERVICENOW_INSTANCE") or ""
        self._client_id = _resolver.resolve("SERVICENOW_CLIENT_ID")
        self._client_secret = _resolver.resolve("SERVICENOW_CLIENT_SECRET")
        self._username = _resolver.resolve("SERVICENOW_USERNAME")
        self._password = _resolver.resolve("SERVICENOW_PASSWORD")
        self._token: str | None = None
        self._expires_at: float = 0.0
        self._use_oauth = bool(self._client_id and self._client_secret)

    def get_token(self) -> str:
        if not self._use_oauth:
            # Basic auth — return password as the "token"
            return self._password or ""
        with self._lock:
            if self._token and time.time() < self._expires_at - 60:
                return self._token
            self._refresh()
            return self._token  # type: ignore[return-value]

    def get_auth_header(self) -> dict[str, str]:
        if not self._use_oauth:
            import base64

            creds = base64.b64encode(
                f"{self._username}:{self._password}".encode()
            ).decode()
            return {"Authorization": f"Basic {creds}"}
        return {"Authorization": f"Bearer {self.get_token()}"}

    def _refresh(self) -> None:
        import httpx

        resp = httpx.post(
            f"{self._instance}/oauth_token.do",
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._expires_at = time.time() + int(data.get("expires_in", 1800))
        logger.debug("Refreshed ServiceNow OAuth token")


# ---------------------------------------------------------------------------
# SonarQube Token Manager
# ---------------------------------------------------------------------------

class SonarQubeTokenManager(TokenManager):
    """Static API token. Supports token auth (Bearer) and basic auth.

    Requires: SONARQUBE_TOKEN
    """

    def __init__(self) -> None:
        super().__init__()
        self._token = _resolver.resolve("SONARQUBE_TOKEN") or ""
        if not self._token:
            raise RuntimeError("SonarQubeTokenManager requires SONARQUBE_TOKEN")

    def get_token(self) -> str:
        return self._token

    def get_auth_header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}


# ---------------------------------------------------------------------------
# Jira Token Manager
# ---------------------------------------------------------------------------

class JiraTokenManager(TokenManager):
    """Supports Jira Cloud (email + API token) and Jira Data Center (PAT).

    Cloud: JIRA_EMAIL + JIRA_API_TOKEN → basic auth
    Data Center: JIRA_PAT → Bearer auth
    """

    def __init__(self) -> None:
        super().__init__()
        self._email = _resolver.resolve("JIRA_EMAIL")
        self._api_token = _resolver.resolve("JIRA_API_TOKEN")
        self._pat = _resolver.resolve("JIRA_PAT")
        self._use_pat = bool(self._pat)

        if not self._use_pat and not (self._email and self._api_token):
            raise RuntimeError(
                "JiraTokenManager requires JIRA_PAT (Data Center) or "
                "JIRA_EMAIL + JIRA_API_TOKEN (Cloud)"
            )

    def get_token(self) -> str:
        if self._use_pat:
            return self._pat  # type: ignore[return-value]
        return self._api_token  # type: ignore[return-value]

    def get_auth_header(self) -> dict[str, str]:
        if self._use_pat:
            return {"Authorization": f"Bearer {self._pat}"}
        import base64

        creds = base64.b64encode(
            f"{self._email}:{self._api_token}".encode()
        ).decode()
        return {"Authorization": f"Basic {creds}"}
