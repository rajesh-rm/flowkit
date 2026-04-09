"""SonarQube project details — enriched metadata from /api/components/show.

Returns description, visibility, version, tags, and analysis dates that are
not available from the lightweight /api/components/search endpoint used by
SonarQubeProjects. Entity-parallel: fans out by project key.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import SonarQubeAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec


@register
class SonarQubeProjectDetails(SonarQubeAsset):
    """Enriched project metadata — description, visibility, tags, versions."""

    name = "sonarqube_project_details"
    target_table = "sonarqube_project_details"

    pagination_config = PaginationConfig(strategy="none")

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_projects"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("key", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("description", "TEXT"),
        Column("visibility", "TEXT"),
        Column("version", "TEXT"),
        Column("analysis_date", "TIMESTAMPTZ"),
        Column("leak_period_date", "TIMESTAMPTZ"),
        Column("tags", "JSONB"),
    ]

    primary_key = ["key"]
    indexes = [
        Index(columns=("analysis_date",)),
    ]

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        return RequestSpec(
            method="GET",
            url=f"{self.api_url}/api/components/show",
            params={"component": entity_key},
        )

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        component = response.get("component", {})
        key = component.get("key")
        if not key:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        tags = component.get("tags")
        row = {
            "key": key,
            "name": component.get("name"),
            "description": component.get("description"),
            "visibility": component.get("visibility"),
            "version": component.get("version"),
            "analysis_date": component.get("analysisDate"),
            "leak_period_date": component.get("leakPeriodDate"),
            "tags": json.dumps(tags) if tags else None,
        }

        return pd.DataFrame([row]), PaginationState(has_more=False)
