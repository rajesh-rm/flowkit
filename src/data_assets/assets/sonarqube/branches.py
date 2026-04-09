"""SonarQube project branches — per-project branch list with quality gate status.

Uses /api/project_branches/list which returns all branches in a single
unpaginated response. Entity-parallel: fans out by project key from
sonarqube_projects.
"""

from __future__ import annotations

import os
from typing import Any

import pandas as pd

from data_assets.assets.sonarqube.helpers import SonarQubeAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec


@register
class SonarQubeBranches(SonarQubeAsset):
    """Branches per project with quality gate status and last analysis date."""

    name = "sonarqube_branches"
    target_table = "sonarqube_branches"

    pagination_config = PaginationConfig(strategy="none")

    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 3

    parent_asset_name = "sonarqube_projects"
    entity_key_column = "project_key"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    columns = [
        Column("project_key", "TEXT", nullable=False),
        Column("name", "TEXT", nullable=False),
        Column("is_main", "BOOLEAN"),
        Column("type", "TEXT"),
        Column("quality_gate_status", "TEXT"),
        Column("analysis_date", "TIMESTAMPTZ"),
        Column("excluded_from_purge", "BOOLEAN"),
    ]

    primary_key = ["project_key", "name"]
    indexes = [
        Index(columns=("analysis_date",)),
    ]

    def build_entity_request(
        self, entity_key: str, context: RunContext, checkpoint: dict | None = None
    ) -> RequestSpec:
        base = os.environ.get("SONARQUBE_URL", self.base_url)
        return RequestSpec(
            method="GET",
            url=f"{base}/api/project_branches/list",
            params={"project": entity_key},
        )

    def parse_response(self, response: Any) -> tuple[pd.DataFrame, PaginationState]:
        branches = response.get("branches", [])
        if not branches:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        rows = []
        for b in branches:
            rows.append({
                "name": b.get("name"),
                "is_main": b.get("isMain"),
                "type": b.get("type"),
                "quality_gate_status": (b.get("status") or {}).get("qualityGateStatus"),
                "analysis_date": b.get("analysisDate"),
                "excluded_from_purge": b.get("excludedFromPurge"),
            })

        return pd.DataFrame(rows), PaginationState(has_more=False)
