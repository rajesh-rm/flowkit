from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column
from data_assets.core.registry import register


@register
class ServiceNowHardwareAssets(ServiceNowTableAsset):
    """ServiceNow hardware assets via Table API."""

    name = "servicenow_hardware_assets"
    target_table = "servicenow_hardware_assets"
    table_name = "alm_hardware"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("display_name", "TEXT"),
        Column("asset_tag", "TEXT"),
        Column("serial_number", "TEXT"),
        Column("model", "TEXT"),
        Column("model_category", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("location", "TEXT"),
        Column("install_status", "TEXT"),
        Column("substatus", "TEXT"),
        Column("ci", "TEXT"),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
