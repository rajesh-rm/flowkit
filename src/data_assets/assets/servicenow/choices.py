from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register


@register
class ServiceNowChoices(ServiceNowTableAsset):
    """ServiceNow dropdown decode table via Table API.

    Decodes raw coded values (e.g., incident.state="1") to human-readable
    labels (e.g., "New"). Full-replaced each run since it is a reference
    table with no reliable incremental sync.
    """

    name = "servicenow_choices"
    target_table = "servicenow_choices"
    table_name = "sys_choice"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL
    date_column = None

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("element", "TEXT"),
        Column("value", "TEXT"),
        Column("label", "TEXT"),
        Column("language", "TEXT"),
        Column("inactive", "TEXT"),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("name", "element")),
        Index(columns=("name", "element", "value")),
    ]
