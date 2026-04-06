from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register


@register
class ServiceNowLocations(ServiceNowTableAsset):
    """ServiceNow locations via Table API."""

    name = "servicenow_locations"
    target_table = "servicenow_locations"
    table_name = "cmn_location"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("city", "TEXT"),
        Column("state", "TEXT"),
        Column("country", "TEXT"),
        Column("latitude", "TEXT"),
        Column("longitude", "TEXT"),
        Column("parent", "TEXT"),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("name",)),
        Index(columns=("country",)),
        Index(columns=("sys_updated_on",)),
    ]
