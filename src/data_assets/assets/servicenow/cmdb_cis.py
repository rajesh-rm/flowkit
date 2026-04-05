from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column
from data_assets.core.registry import register


@register
class ServiceNowCmdbCIs(ServiceNowTableAsset):
    """ServiceNow CMDB configuration items via Table API."""

    name = "servicenow_cmdb_cis"
    target_table = "servicenow_cmdb_cis"
    table_name = "cmdb_ci"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("sys_class_name", "TEXT"),
        Column("category", "TEXT"),
        Column("subcategory", "TEXT"),
        Column("operational_status", "TEXT"),
        Column("asset_tag", "TEXT"),
        Column("serial_number", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("support_group", "TEXT"),
        Column("company", "TEXT"),
        Column("location", "TEXT"),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
