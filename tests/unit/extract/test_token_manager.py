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
