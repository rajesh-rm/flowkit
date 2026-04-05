from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column
from data_assets.core.registry import register


@register
class ServiceNowCatalogRequests(ServiceNowTableAsset):
    """ServiceNow service catalog requests via Table API."""

    name = "servicenow_catalog_requests"
    target_table = "servicenow_catalog_requests"
    table_name = "sc_request"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("request_state", "TEXT"),
        Column("requested_for", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
