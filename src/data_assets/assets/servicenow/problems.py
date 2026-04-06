from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register


@register
class ServiceNowProblems(ServiceNowTableAsset):
    """ServiceNow problem records via Table API."""

    name = "servicenow_problems"
    target_table = "servicenow_problems"
    table_name = "problem"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("state", "TEXT"),
        Column("priority", "TEXT"),
        Column("category", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("assignment_group", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("state",)),
        Index(columns=("priority",)),
        Index(columns=("assignment_group",)),
        Index(columns=("sys_updated_on",)),
    ]
