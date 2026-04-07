"""Shared fixtures for unit tests — eliminates duplication across test files."""

from __future__ import annotations

import uuid

import pytest

from data_assets.core.enums import RunMode
from data_assets.core.run_context import RunContext


# ---------------------------------------------------------------------------
# RunContext factory
# ---------------------------------------------------------------------------


def make_ctx(**kwargs) -> RunContext:
    """Create a RunContext with sensible defaults for testing."""
    defaults = {
        "run_id": uuid.uuid4(),
        "mode": RunMode.FULL,
        "asset_name": "test",
        "partition_key": "",
    }
    defaults.update(kwargs)
    return RunContext(**defaults)


# ---------------------------------------------------------------------------
# Source-specific environment fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def github_env(monkeypatch):
    """Set all GitHub env vars needed to instantiate GitHub assets."""
    monkeypatch.setenv("GITHUB_APP_ID", "1")
    monkeypatch.setenv("GITHUB_PRIVATE_KEY", "k")
    monkeypatch.setenv("GITHUB_INSTALLATION_ID", "2")
    monkeypatch.setenv("GITHUB_ORGS", "org-one,org-two")


@pytest.fixture
def jira_env(monkeypatch):
    """Set all Jira env vars needed to instantiate Jira assets."""
    monkeypatch.setenv("JIRA_URL", "https://jira.test")
    monkeypatch.setenv("JIRA_EMAIL", "a@b.com")
    monkeypatch.setenv("JIRA_API_TOKEN", "tok")


@pytest.fixture
def sonarqube_env(monkeypatch):
    """Set all SonarQube env vars needed to instantiate SonarQube assets."""
    monkeypatch.setenv("SONARQUBE_URL", "https://sonar.test")
    monkeypatch.setenv("SONARQUBE_TOKEN", "fake")


@pytest.fixture
def servicenow_env(monkeypatch):
    """Set all ServiceNow env vars needed to instantiate ServiceNow assets."""
    monkeypatch.setenv("SERVICENOW_INSTANCE", "https://dev.service-now.com")
    monkeypatch.setenv("SERVICENOW_USERNAME", "admin")
    monkeypatch.setenv("SERVICENOW_PASSWORD", "pass")
