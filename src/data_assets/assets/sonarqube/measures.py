"""SonarQube project measures — current metric values per project per branch.

Uses /api/measures/component to fetch key quality metrics (bugs, vulnerabilities,
code_smells, coverage, ncloc, new_coverage, etc.) for each project branch.
Entity-parallel: fans out by (project_key, branch) from sonarqube_branches.

Response structure (standard metrics):
    {"component": {"key": "...", "measures": [{"metric": "bugs", "value": "3"}, ...]}}

Response structure (new-code metrics — value nested in ``period``):
    {"metric": "new_coverage", "period": {"index": 1, "value": "100.0"}}

Each measure is stored as one row per (project, branch, metric) in EAV format.
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
from sqlalchemy import DateTime, Text


@register
class SonarQubeMeasures(SonarQubeAsset):
    """Current quality measures per project per branch — one row per (project, branch, metric)."""

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
        Column("metric_key", Text(), nullable=False),
        Column("metric_value", Text()),
        Column("collected_at", DateTime(timezone=True)),
    ]

    primary_key = ["project_key", "branch", "metric_key"]
    column_null_thresholds = {"metric_value": 1.0}  # EAV: new-code metrics often null
    indexes = [
        Index(columns=("metric_key",)),
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

        if not project_key:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        valid_metrics = set(ALL_METRICS)
        rows: list[dict[str, Any]] = []
        now = pd.Timestamp.now(tz="UTC")

        for m in measures_list:
            metric = m.get("metric")
            # Standard metrics use top-level "value"; new-code metrics
            # nest the value inside "period.value".
            value = m.get("value")
            if value is None:
                value = m.get("period", {}).get("value")
            if metric in valid_metrics:
                rows.append({
                    "project_key": project_key,
                    "metric_key": metric,
                    "metric_value": value,
                    "collected_at": now,
                })

        if rows:
            df = pd.DataFrame(rows)
        else:
            df = pd.DataFrame(columns=[c.name for c in self.columns])

        return df, PaginationState(has_more=False)
