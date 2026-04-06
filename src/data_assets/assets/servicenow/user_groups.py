from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register


@register
class ServiceNowUserGroups(ServiceNowTableAsset):
    """ServiceNow user groups via Table API."""

    name = "servicenow_user_groups"
    target_table = "servicenow_user_groups"
    table_name = "sys_user_group"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("description", "TEXT"),
        Column("manager", "TEXT"),
        Column("email", "TEXT"),
        Column("active", "TEXT"),
        Column("type", "TEXT"),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("name",)),
        Index(columns=("active",)),
        Index(columns=("sys_updated_on",)),
    ]
