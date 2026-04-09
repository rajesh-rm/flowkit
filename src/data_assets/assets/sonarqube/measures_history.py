"""SonarQube measures history — metric trends over time per project.

Uses /api/measures/search_history to fetch historical values for key quality
metrics. The API returns a metric-grouped response that is flattened into
one row per (project_key, metric, date) for relational storage.

Entity-parallel: fans out by project key from sonarqube_projects.
Incremental: uses the `from` parameter to fetch only new data points
since the last watermark.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import DEFAULT_METRICS, SonarQubeAsset, parse_paging
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec


@register
class SonarQubeMeasuresHistory(SonarQubeAsset):
    """Historical quality metrics per project — one row per metric per date."""

    name = "sonarqube_measures_history"
    target_table = "sonarqube_measures_history"

    pagination_config = PaginationConfig(
        strategy="page_number", page_size=100, total_path="paging.total",
    )

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_projects"
    entity_key_column = "project_key"

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD
    date_column = "date"

    columns = [
        Column("project_key", "TEXT", nullable=False),
        Column("metric", "TEXT", nullable=False),
        Column("date", "TIMESTAMPTZ", nullable=False),
        Column("value", "TEXT"),
    ]

    primary_key = ["project_key", "metric", "date"]
    indexes = [
        Index(columns=("date",)),
        Index(columns=("metric",)),
    ]

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        params: dict[str, Any] = {
            "component": entity_key,
            "metrics": ",".join(DEFAULT_METRICS),
            "ps": 100,
            "p": page,
        }
        if context.start_date:
            params["from"] = context.start_date.strftime("%Y-%m-%dT%H:%M:%S%z")

        return RequestSpec(method="GET", url=f"{self.api_url}/api/measures/search_history", params=params)

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        rows: list[dict] = []
        for measure in response.get("measures", []):
            metric = measure.get("metric", "")
            for entry in measure.get("history", []):
                rows.append({
                    "metric": metric,
                    "date": entry.get("date"),
                    "value": entry.get("value"),
                })

        if not rows:
            return pd.DataFrame(columns=[c.name for c in self.columns]), parse_paging(response)

        return pd.DataFrame(rows), parse_paging(response)
