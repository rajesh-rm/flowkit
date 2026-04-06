"""Transform asset: daily incident summary from raw ServiceNow incidents."""

from __future__ import annotations

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.transform_asset import TransformAsset


@register
class IncidentSummary(TransformAsset):
    name = "incident_summary"
    description = "Daily incident summary aggregated from raw ServiceNow incidents"
    target_schema = "mart"
    target_table = "incident_summary"
    source_tables = ["servicenow_incidents"]
    default_run_mode = RunMode.TRANSFORM
    load_strategy = LoadStrategy.FULL_REPLACE

    columns = [
        Column("report_date", "DATE", nullable=False),
        Column("priority", "TEXT", nullable=False),
        Column("state", "TEXT", nullable=False),
        Column("incident_count", "INTEGER", nullable=False),
    ]
    primary_key = ["report_date", "priority", "state"]
    indexes = [
        Index(columns=("report_date",)),
        Index(columns=("priority",)),
    ]

    def query(self, context: RunContext) -> str:
        return """
            SELECT
                DATE(opened_at) AS report_date,
                COALESCE(priority, 'Unknown') AS priority,
                COALESCE(state, 'Unknown') AS state,
                COUNT(*) AS incident_count
            FROM raw.servicenow_incidents
            WHERE opened_at IS NOT NULL
            GROUP BY DATE(opened_at), priority, state
            ORDER BY report_date DESC, priority, state
        """
