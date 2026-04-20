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

import logging
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

logger = logging.getLogger(__name__)


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
    column_null_thresholds = {"value": 1.0}  # new_* metrics may lack values on some analyses
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

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        null_mask = df["branch"].isnull()
        if null_mask.any():
            n = int(null_mask.sum())
            sample = (
                df.loc[null_mask, ["project_key", "metric_key", "analysis_date"]]
                .head(3)
                .to_dict(orient="records")
            )
            logger.warning(
                "sonarqube_measures_history: dropping %d rows with null branch (sample: %s)",
                n, sample,
            )
            df = df.loc[~null_mask].copy()
            if df.empty:
                return df

        raw = df["analysis_date"].astype("string")
        sort_ts = pd.to_datetime(raw, utc=True, errors="coerce")
        nat_count = int(sort_ts.isna().sum())
        if nat_count > 0:
            bad = raw[sort_ts.isna()].head(3).tolist()
            logger.warning(
                "sonarqube_measures_history: %d analysis_date values failed to parse (sample: %s)",
                nat_count, bad,
            )

        df = df.assign(
            _sort_ts=sort_ts,
            analysis_date=pd.to_datetime(raw.str.slice(0, 10), utc=True, errors="coerce"),
        )

        df = (
            df.sort_values(
                by=["project_key", "branch", "metric_key", "analysis_date", "_sort_ts"],
                ascending=[True, True, True, True, False],
                na_position="last",
            )
            .drop_duplicates(subset=self.primary_key, keep="first")
            .drop(columns=["_sort_ts"])
            .reset_index(drop=True)
        )
        return df
