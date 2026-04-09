"""SonarQube project analyses and analysis events.

Two assets sharing the same API endpoint (/api/project_analyses/search):

- SonarQubeAnalyses — one row per scan (key, date, version, revision, CI)
- SonarQubeAnalysisEvents — one row per event (quality gate changes, profile changes)

Both are entity-parallel on sonarqube_projects. The API is called once per
asset per project (twice total), which is acceptable at 5 req/sec and keeps
both assets in the standard extraction pipeline without custom extract overrides.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import SonarQubeAsset, parse_paging
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec


# ---------------------------------------------------------------------------
# Asset: Analyses (scan history)
# ---------------------------------------------------------------------------

@register
class SonarQubeAnalyses(SonarQubeAsset):
    """Scan history per project — one row per analysis."""

    name = "sonarqube_analyses"
    target_table = "sonarqube_analyses"

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
        Column("key", "TEXT", nullable=False),
        Column("project_key", "TEXT", nullable=False),
        Column("date", "TIMESTAMPTZ"),
        Column("project_version", "TEXT"),
        Column("revision", "TEXT"),
        Column("detected_ci", "TEXT"),
    ]

    primary_key = ["key"]
    indexes = [
        Index(columns=("project_key",)),
        Index(columns=("date",)),
    ]

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        return RequestSpec(
            method="GET",
            url=f"{self.api_url}/api/project_analyses/search",
            params={"project": entity_key, "ps": 100, "p": page},
        )

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        analyses = response.get("analyses", [])
        if not analyses:
            return pd.DataFrame(columns=[c.name for c in self.columns]), parse_paging(response)

        rename = {"projectVersion": "project_version", "detectedCI": "detected_ci"}
        df = pd.DataFrame(analyses)
        df = df.rename(columns=rename)
        df = df.drop(columns=["events"], errors="ignore")
        keep = [c for c in df.columns if c in {col.name for col in self.columns}]
        df = df[keep]

        return df, parse_paging(response)


# ---------------------------------------------------------------------------
# Asset: Analysis Events (quality gate changes, profile changes)
# ---------------------------------------------------------------------------

@register
class SonarQubeAnalysisEvents(SonarQubeAsset):
    """Events from project analyses — quality gate changes, profile updates."""

    name = "sonarqube_analysis_events"
    target_table = "sonarqube_analysis_events"

    pagination_config = PaginationConfig(
        strategy="page_number", page_size=100, total_path="paging.total",
    )

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_projects"
    entity_key_column = "project_key"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("key", "TEXT", nullable=False),
        Column("analysis_key", "TEXT", nullable=False),
        Column("project_key", "TEXT", nullable=False),
        Column("category", "TEXT"),
        Column("name", "TEXT"),
        Column("description", "TEXT"),
        Column("details", "JSONB"),
    ]

    primary_key = ["key"]
    indexes = [
        Index(columns=("analysis_key",)),
        Index(columns=("project_key",)),
        Index(columns=("category",)),
    ]

    # Known category-specific payload fields to merge into `details`
    _DETAIL_KEYS = {"qualityGate", "definitionChange", "qualityProfile"}

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        page = (checkpoint.get("next_page") or 1) if checkpoint else 1
        return RequestSpec(
            method="GET",
            url=f"{self.api_url}/api/project_analyses/search",
            params={"project": entity_key, "ps": 100, "p": page},
        )

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        analyses = response.get("analyses", [])
        rows: list[dict] = []

        for analysis in analyses:
            analysis_key = analysis.get("key", "")
            for event in analysis.get("events", []):
                detail_payload = {
                    k: v for k, v in event.items() if k in self._DETAIL_KEYS
                }
                rows.append({
                    "key": event.get("key"),
                    "analysis_key": analysis_key,
                    "category": event.get("category"),
                    "name": event.get("name"),
                    "description": event.get("description"),
                    "details": json.dumps(detail_payload) if detail_payload else None,
                })

        if not rows:
            return pd.DataFrame(columns=[c.name for c in self.columns]), parse_paging(response)

        return pd.DataFrame(rows), parse_paging(response)
