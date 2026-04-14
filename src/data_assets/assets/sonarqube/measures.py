"""SonarQube project measures — current metric values per project per branch.

Uses /api/measures/component to fetch key quality metrics (bugs, vulnerabilities,
code_smells, coverage, ncloc, new_coverage, etc.) for each project branch.
Entity-parallel: fans out by (project_key, branch) from sonarqube_branches.

Response structure:
    {"component": {"key": "...", "measures": [{"metric": "bugs", "value": "3"}, ...]}}

Each measure is flattened into one row per project+branch with metric columns.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import ALL_METRICS, SonarQubeAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from sqlalchemy import BigInteger, DateTime, Float, Integer, Text


@register
class SonarQubeMeasures(SonarQubeAsset):
    """Current quality measures per project per branch — one row per (project, branch)."""

    name = "sonarqube_measures"
    target_table = "sonarqube_measures"

    # No pagination — one request per (project, branch) returns all metrics
    pagination_config = PaginationConfig(strategy="none")

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_branches"
    entity_key_column = None
    entity_key_map = {"name": "branch"}

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("project_key", Text(), nullable=False),
        Column("branch", Text(), nullable=False),
        Column("ncloc", BigInteger()),
        Column("bugs", Integer()),
        Column("vulnerabilities", Integer()),
        Column("code_smells", Integer()),
        Column("coverage", Float()),
        Column("duplicated_lines_density", Float()),
        Column("sqale_index", BigInteger()),
        Column("new_coverage", Float()),
        Column("new_lines_to_cover", BigInteger()),
        Column("new_line_coverage", Float()),
        Column("collected_at", DateTime(timezone=True)),
    ]

    primary_key = ["project_key", "branch"]
    indexes = [
        Index(columns=("ncloc",)),
        Index(columns=("branch",)),
    ]

    def build_entity_request(
        self, entity_key: Any, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        project_key = entity_key["project_key"]
        branch = entity_key["name"]
        return RequestSpec(
            method="GET",
            url=f"{self.api_url}/api/measures/component",
            params={
                "component": project_key,
                "branch": branch,
                "metricKeys": ",".join(ALL_METRICS),
            },
        )

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        component = response.get("component", {})
        project_key = component.get("key", "")
        measures_list = component.get("measures", [])

        valid_metrics = set(ALL_METRICS)
        row: dict[str, Any] = {"project_key": project_key}
        for m in measures_list:
            metric = m.get("metric")
            value = m.get("value")
            if metric in valid_metrics:
                row[metric] = value

        if project_key:
            row["collected_at"] = pd.Timestamp.now(tz="UTC")
            df = pd.DataFrame([row])
        else:
            df = pd.DataFrame(columns=[c.name for c in self.columns])

        return df, PaginationState(has_more=False)
