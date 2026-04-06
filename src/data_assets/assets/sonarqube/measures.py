"""SonarQube project measures — current metric values per project.

Uses /api/measures/component to fetch key quality metrics (bugs, vulnerabilities,
code_smells, coverage, ncloc, etc.) for each project. Entity-parallel: fans out
by project key from sonarqube_projects.

Response structure:
    {"component": {"key": "...", "measures": [{"metric": "bugs", "value": "3"}, ...]}}

Each measure is flattened into one row per project with metric columns.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import SonarQubeAsset
from data_assets.core.column import Column
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec

# Metrics to fetch — covers the core quality dimensions
DEFAULT_METRICS = [
    "ncloc", "bugs", "vulnerabilities", "code_smells",
    "coverage", "duplicated_lines_density", "sqale_index",
]


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
        Column("project_key", "TEXT", nullable=False),
        Column("ncloc", "BIGINT"),
        Column("bugs", "INTEGER"),
        Column("vulnerabilities", "INTEGER"),
        Column("code_smells", "INTEGER"),
        Column("coverage", "FLOAT"),
        Column("duplicated_lines_density", "FLOAT"),
        Column("sqale_index", "BIGINT"),
    ]

    primary_key = ["project_key"]

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        base = os.environ.get("SONARQUBE_URL", self.base_url)
        return RequestSpec(
            method="GET",
            url=f"{base}/api/measures/component",
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
