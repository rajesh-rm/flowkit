"""Shared config for SonarQube assets."""

from __future__ import annotations

import math
import os

from data_assets.core.api_asset import APIAsset
from data_assets.core.types import PaginationState
from data_assets.extract.token_manager import SonarQubeTokenManager

# Quality metrics tracked by SonarQubeMeasures and SonarQubeMeasuresHistory.
# Single source of truth — both assets import from here.
DEFAULT_METRICS = [
    "ncloc", "bugs", "vulnerabilities", "code_smells",
    "coverage", "duplicated_lines_density", "sqale_index",
]


def parse_paging(response: dict) -> PaginationState:
    """Extract standard SonarQube paging state from a response."""
    paging = response.get("paging", {})
    total = paging.get("total", 0)
    page_index = paging.get("pageIndex", 1)
    page_size = paging.get("pageSize", 100)
    total_pages = math.ceil(total / page_size) if page_size else 1
    return PaginationState(
        has_more=page_index < total_pages,
        next_page=page_index + 1,
        total_pages=total_pages,
        total_records=total,
    )


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

    column_max_lengths = {
        "key": 500,            # SonarQube project/issue key — API docs say 400
        "project_key": 500,
        "project": 500,
        "rule": 500,
        "severity": 100,
        "status": 100,
        "type": 100,
        "category": 200,
    }

    @property
    def api_url(self) -> str:
        """Resolve the SonarQube base URL (env var overrides class default)."""
        return os.environ.get("SONARQUBE_URL", self.base_url)
