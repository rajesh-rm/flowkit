"""Shared config for SonarQube assets."""

from __future__ import annotations

from data_assets.core.api_asset import APIAsset
from data_assets.extract.token_manager import SonarQubeTokenManager


class SonarQubeAsset(APIAsset):
    """Base class for SonarQube assets using APIAsset.

    Provides shared source config: token manager, rate limit, schema.
    SonarQubeProjects uses RestAsset instead (declarative) and sets
    these attributes directly.
    """

    source_name = "sonarqube"
    target_schema = "raw"
    token_manager_class = SonarQubeTokenManager
    rate_limit_per_second = 5.0
