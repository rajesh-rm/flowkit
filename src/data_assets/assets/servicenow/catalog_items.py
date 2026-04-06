from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register


@register
class ServiceNowCatalogItems(ServiceNowTableAsset):
    """ServiceNow service catalog request items via Table API."""

    name = "servicenow_catalog_items"
    target_table = "servicenow_catalog_items"
    table_name = "sc_req_item"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("request", "TEXT"),
        Column("cat_item", "TEXT"),
        Column("state", "TEXT"),
        Column("quantity", "TEXT"),
        Column("price", "TEXT"),
        Column("stage", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("request",)),
        Index(columns=("state",)),
        Index(columns=("sys_updated_on",)),
    ]
