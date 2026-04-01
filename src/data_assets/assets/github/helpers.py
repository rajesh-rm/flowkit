"""Shared helpers for GitHub assets."""

from __future__ import annotations

import os


def get_github_org() -> str:
    """Return the first GitHub org from GITHUB_ORGS env var."""
    orgs = [o.strip() for o in os.environ.get("GITHUB_ORGS", "").split(",") if o.strip()]
    if not orgs:
        raise RuntimeError("GITHUB_ORGS env var is not set or empty")
    return orgs[0]


def get_github_base_url() -> str:
    """Return the GitHub API base URL (supports Enterprise override)."""
    return os.environ.get("GITHUB_API_URL", "https://api.github.com")


def filter_to_current_org(keys: list) -> list:
    """Filter entity keys (repo full_names) to the current org only."""
    org = os.environ.get("GITHUB_ORGS", "").split(",")[0].strip()
    if not org:
        return keys
    return [k for k in keys if str(k).startswith(f"{org}/")]
