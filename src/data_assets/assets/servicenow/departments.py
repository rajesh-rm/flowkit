from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register


@register
class ServiceNowDepartments(ServiceNowTableAsset):
    """ServiceNow departments via Table API."""

    name = "servicenow_departments"
    target_table = "servicenow_departments"
    table_name = "cmn_department"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("dept_head", "TEXT"),
        Column("description", "TEXT"),
        Column("parent", "TEXT"),
        Column("primary_contact", "TEXT"),
        Column("company", "TEXT"),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("name",)),
        Index(columns=("sys_updated_on",)),
    ]
