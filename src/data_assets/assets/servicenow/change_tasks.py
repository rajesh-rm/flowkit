from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column
from data_assets.core.registry import register


@register
class ServiceNowChangeTasks(ServiceNowTableAsset):
    """ServiceNow change tasks via Table API."""

    name = "servicenow_change_tasks"
    target_table = "servicenow_change_tasks"
    table_name = "change_task"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("state", "TEXT"),
        Column("priority", "TEXT"),
        Column("change_request", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("assignment_group", "TEXT"),
        Column("planned_start_date", "TIMESTAMPTZ", nullable=True),
        Column("planned_end_date", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
