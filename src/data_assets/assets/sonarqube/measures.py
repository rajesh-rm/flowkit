"""SonarQube project measures — current metric values per project.

Uses /api/measures/component to fetch key quality metrics (bugs, vulnerabilities,
code_smells, coverage, ncloc, etc.) for each project. Entity-parallel: fans out
by project key from sonarqube_projects.

Response structure:
    {"component": {"key": "...", "measures": [{"metric": "bugs", "value": "3"}, ...]}}

Each measure is flattened into one row per project with metric columns.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import DEFAULT_METRICS, SonarQubeAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from sqlalchemy import BigInteger, Float, Integer, Text


@register
class SonarQubeMeasures(SonarQubeAsset):
    """Current quality measures per project — one row per project."""

    name = "sonarqube_measures"
    target_table = "sonarqube_measures"

    # No pagination — one request per project returns all metrics
    pagination_config = PaginationConfig(strategy="none")

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_projects"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("project_key", Text(), nullable=False),
        Column("ncloc", BigInteger()),
        Column("bugs", Integer()),
        Column("vulnerabilities", Integer()),
        Column("code_smells", Integer()),
        Column("coverage", Float()),
        Column("duplicated_lines_density", Float()),
        Column("sqale_index", BigInteger()),
    ]

    primary_key = ["project_key"]
    indexes = [
        Index(columns=("ncloc",)),
    ]

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        return RequestSpec(
            method="GET",
            url=f"{self.api_url}/api/measures/component",
            params={
                "component": entity_key,
                "metricKeys": ",".join(DEFAULT_METRICS),
            },
        )

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        component = response.get("component", {})
        project_key = component.get("key", "")
        measures_list = component.get("measures", [])

        row: dict[str, Any] = {"project_key": project_key}
        for m in measures_list:
            metric = m.get("metric")
            value = m.get("value")
            if metric in {c.name for c in self.columns} and metric != "project_key":
                row[metric] = value

        df = pd.DataFrame([row]) if project_key else pd.DataFrame(columns=[c.name for c in self.columns])
        return df, PaginationState(has_more=False)
