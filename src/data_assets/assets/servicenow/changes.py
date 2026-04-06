from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register


@register
class ServiceNowChanges(ServiceNowTableAsset):
    """ServiceNow change requests via Table API with keyset pagination."""

    name = "servicenow_changes"
    target_table = "servicenow_changes"
    table_name = "change_request"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("description", "TEXT"),
        Column("state", "TEXT"),
        Column("type", "TEXT"),
        Column("priority", "TEXT"),
        Column("risk", "TEXT"),
        Column("category", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("assignment_group", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("start_date", "TIMESTAMPTZ", nullable=True),
        Column("end_date", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("state",)),
        Index(columns=("priority",)),
        Index(columns=("assignment_group",)),
        Index(columns=("opened_at",)),
        Index(columns=("sys_updated_on",)),
    ]
