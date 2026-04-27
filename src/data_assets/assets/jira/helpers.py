"""Shared base class for Jira assets."""

from __future__ import annotations

import os

from data_assets.core.api_asset import APIAsset
from data_assets.extract.token_manager import JiraTokenManager


class JiraAsset(APIAsset):
    """Base for Jira assets. Shares source config and URL resolution."""

    source_name = "jira"
    target_schema = "raw"
    token_manager_class = JiraTokenManager
    base_url = ""
    rate_limit_per_second = 5.0

    # Default — subclasses with PII columns (assignee, reporter, etc.) must
    # set this to True and mark the relevant Column(sensitive=True).
    contains_sensitive_data = False

    def get_jira_url(self) -> str:
        url = os.environ.get("JIRA_URL", self.base_url)
        if not url:
            raise RuntimeError(
                "Jira URL not configured. Set the JIRA_URL environment variable "
                "or override base_url on your asset class."
            )
        return url
