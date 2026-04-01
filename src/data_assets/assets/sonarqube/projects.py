"""SonarQube projects via /api/components/search — uses RestAsset for declarative config.

Lists all projects (qualifier=TRK) from the SonarQube instance. This is the
simplest example of a RestAsset. Compare with sonarqube/issues.py which uses
APIAsset (custom) because it needs sort-by-update logic.

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
    endpoint = "/api/components/search"
    rate_limit_per_second = 5.0

    # --- Response parsing ---
    response_path = "components"  # JSON path to the records array
    pagination = {
        "strategy": "page_number",
        "page_size": 100,
        "total_path": "paging.total",
        "page_index_path": "paging.pageIndex",
    }

    # --- Parallelism ---
    parallel_mode = ParallelMode.PAGE_PARALLEL
    max_workers = 3

    # --- Load behavior ---
    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    # --- Schema ---
    # /api/components/search returns: key, name, qualifier, project
    columns = [
        Column("key", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("qualifier", "TEXT"),
    ]
    primary_key = ["key"]

    def build_request(self, context, checkpoint=None):
        spec = super().build_request(context, checkpoint)
        # Filter to projects only (TRK = project qualifier in SonarQube)
        spec.params["qualifiers"] = "TRK"
        return spec
