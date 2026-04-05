from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column
from data_assets.core.registry import register


@register
class ServiceNowUsers(ServiceNowTableAsset):
    """ServiceNow user directory via Table API."""

    name = "servicenow_users"
    target_table = "servicenow_users"
    table_name = "sys_user"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("user_name", "TEXT"),
        Column("name", "TEXT"),
        Column("email", "TEXT"),
        Column("title", "TEXT"),
        Column("department", "TEXT"),
        Column("manager", "TEXT"),
        Column("active", "TEXT"),
        Column("locked_out", "TEXT"),
        Column("last_login_time", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
