from __future__ import annotations

import math
import os

import pandas as pd

from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.core.run_context import RunContext
from data_assets.extract.token_manager import SonarQubeTokenManager


@register
class SonarQubeProjects(APIAsset):
    """SonarQube projects asset -- full catalogue of projects on the instance."""

    name = "sonarqube_projects"
    source_name = "sonarqube"

    target_schema = "raw"
    target_table = "sonarqube_projects"

    token_manager_class = SonarQubeTokenManager
    base_url = ""  # Set from SONARQUBE_URL env var at runtime

    rate_limit_per_second = 5.0

    pagination_config = PaginationConfig(
        strategy="page_number",
        page_size=100,
        total_field="paging.total",
    )

    parallel_mode = ParallelMode.PAGE_PARALLEL
    max_workers = 3

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("key", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("qualifier", "TEXT"),
        Column("visibility", "TEXT"),
        Column("last_analysis_date", "TIMESTAMPTZ"),
        Column("revision", "TEXT"),
    ]

    primary_key = ["key"]
    date_column = "last_analysis_date"

    # ------------------------------------------------------------------
    # Extract helpers
    # ------------------------------------------------------------------

    def build_request(
        self,
        context: RunContext,
        checkpoint: dict | None = None,
    ) -> RequestSpec:
        page = checkpoint.get("page", 1) if checkpoint else 1
        base = os.environ.get("SONARQUBE_URL", self.base_url)
        return RequestSpec(
            url=f"{base}/api/projects/search",
            method="GET",
            params={"ps": 100, "p": page},
        )

    def parse_response(
        self,
        response: dict,
    ) -> tuple[pd.DataFrame, PaginationState]:
        paging = response["paging"]
        total = paging["total"]
        page_index = paging["pageIndex"]
        page_size = paging["pageSize"]

        total_pages = math.ceil(total / page_size) if page_size else 1

        df = pd.DataFrame(response["components"])

        pagination_state = PaginationState(
            has_more=page_index < total_pages,
            next_page=page_index + 1,
            total_pages=total_pages,
            total_records=total,
        )

        return df, pagination_state
