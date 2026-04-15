"""SonarQube measures history — metric trends over time per project per branch.

Uses /api/measures/search_history to fetch historical values for key quality
metrics. The API returns a metric-grouped response that is flattened into
one row per (project_key, branch, metric, date) for relational storage.

Entity-parallel: fans out by (project_key, branch) from sonarqube_branches.
Incremental: uses the `from` parameter to fetch only new data points
since the last watermark, bounded by a configurable lookback window.

Note: new_* metrics (new_coverage, etc.) may have history entries without
values on some analyses; those are stored as NULL.
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import HISTORY_METRICS, SonarQubeAsset, parse_paging
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from sqlalchemy import DateTime, Text


@register
class SonarQubeMeasuresHistory(SonarQubeAsset):
    """Historical quality metrics per project per branch — one row per metric per date."""

    name = "sonarqube_measures_history"
    target_table = "sonarqube_measures_history"

    pagination_config = PaginationConfig(
        strategy="page_number", page_size=100, total_path="paging.total",
    )

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_branches"
    entity_key_column = None
    entity_key_map = {"project_key": "project_key", "name": "branch"}

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD
    date_column = "analysis_date"

    columns = [
        Column("project_key", Text(), nullable=False),
        Column("branch", Text(), nullable=False),
        Column("metric_key", Text(), nullable=False),
        Column("analysis_date", DateTime(timezone=True), nullable=False),
        Column("value", Text()),
        Column("collected_at", DateTime(timezone=True)),
    ]

    primary_key = ["project_key", "branch", "metric_key", "analysis_date"]
    indexes = [
        Index(columns=("analysis_date",)),
        Index(columns=("metric_key",)),
        Index(columns=("branch",)),
    ]

    @property
    def history_days_back(self) -> int:
        """Maximum lookback window in days. Override via SONARQUBE_HISTORY_DAYS_BACK."""
        return int(os.environ.get("SONARQUBE_HISTORY_DAYS_BACK", "720"))

    def build_entity_request(
        self, entity_key: Any, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        project_key = entity_key["project_key"]
        branch = entity_key["name"]
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1

        today = date.today()
        from_date = today - timedelta(days=self.history_days_back)
        if context.start_date:
            from_date = max(from_date, context.start_date.date())

        params: dict[str, Any] = {
            "component": project_key,
            "branch": branch,
            "metrics": ",".join(HISTORY_METRICS),
            "ps": 100,
            "p": page,
            "from": from_date.isoformat(),
            "to": today.isoformat(),
        }

        return RequestSpec(method="GET", url=f"{self.api_url}/api/measures/search_history", params=params)

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        rows: list[dict] = []
        now = pd.Timestamp.now(tz="UTC")
        for measure in response.get("measures", []):
            metric = measure.get("metric", "")
            for entry in measure.get("history", []):
                rows.append({
                    "metric_key": metric,
                    "analysis_date": entry.get("date"),
                    "value": entry.get("value"),
                    "collected_at": now,
                })

        if not rows:
            return pd.DataFrame(columns=[c.name for c in self.columns]), parse_paging(response)

        return pd.DataFrame(rows), parse_paging(response)
