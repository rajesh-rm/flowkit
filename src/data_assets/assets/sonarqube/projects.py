"""SonarQube projects — uses RestAsset for declarative config.

This is the simplest example of a RestAsset. Compare with sonarqube/issues.py
which uses APIAsset (custom) because it needs sort-by-update logic.

RestAsset handles build_request() and parse_response() automatically from
the class attributes. You only need to define: endpoint, response_path,
pagination, columns, and field_map (if API fields differ from column names).
"""

from __future__ import annotations

from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.rest_asset import RestAsset
from data_assets.extract.token_manager import SonarQubeTokenManager


@register
class SonarQubeProjects(RestAsset):
    """SonarQube projects — full catalog, refreshed each run."""

    name = "sonarqube_projects"
    source_name = "sonarqube"
    target_schema = "raw"
    target_table = "sonarqube_projects"

    # --- Source config ---
    token_manager_class = SonarQubeTokenManager
    base_url_env = "SONARQUBE_URL"
    endpoint = "/api/projects/search"
    rate_limit_per_second = 5.0

    # --- Response parsing ---
    response_path = "components"  # JSON path to the records array
    pagination = {
        "strategy": "page_number",
        "page_size": 100,
        "total_path": "paging.total",
        "page_index_path": "paging.pageIndex",
    }
    field_map = {
        "lastAnalysisDate": "last_analysis_date",  # API field → column name
    }

    # --- Parallelism ---
    parallel_mode = ParallelMode.PAGE_PARALLEL
    max_workers = 3

    # --- Load behavior ---
    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    # --- Schema ---
    columns = [
        Column("key", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("qualifier", "TEXT"),
        Column("visibility", "TEXT"),
        Column("last_analysis_date", "TIMESTAMPTZ"),
        Column("revision", "TEXT"),
    ]
    primary_key = ["key"]
