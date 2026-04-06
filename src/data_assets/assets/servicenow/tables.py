"""All ServiceNow table assets — one class per table, all in one place.

Each class is a thin schema definition (name, table_name, columns, indexes).
All extraction, pagination, and auth logic lives in ServiceNowTableAsset (base.py).

To add a new ServiceNow table:
  1. Add a new @register class below with name, target_table, table_name, columns, indexes
  2. Import it in __init__.py
  3. Add a JSON fixture in tests/fixtures/servicenow/<table>.json
  4. Run: make test-unit
"""

from __future__ import annotations

from data_assets.assets.servicenow.base import ServiceNowTableAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register

# ---------------------------------------------------------------------------
# ITSM tables
# ---------------------------------------------------------------------------


@register
class ServiceNowIncidents(ServiceNowTableAsset):
    """Incident management records."""

    name = "servicenow_incidents"
    target_table = "servicenow_incidents"
    table_name = "incident"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("description", "TEXT"),
        Column("state", "TEXT"),
        Column("priority", "TEXT"),
        Column("severity", "TEXT"),
        Column("category", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("assignment_group", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("state",)),
        Index(columns=("priority",)),
        Index(columns=("assignment_group",)),
        Index(columns=("opened_at",)),
        Index(columns=("sys_updated_on",)),
    ]


@register
class ServiceNowChanges(ServiceNowTableAsset):
    """Change request records."""

    name = "servicenow_changes"
    target_table = "servicenow_changes"
    table_name = "change_request"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("description", "TEXT"),
        Column("state", "TEXT"),
        Column("type", "TEXT"),
        Column("priority", "TEXT"),
        Column("risk", "TEXT"),
        Column("category", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("assignment_group", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("start_date", "TIMESTAMPTZ", nullable=True),
        Column("end_date", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("state",)),
        Index(columns=("priority",)),
        Index(columns=("assignment_group",)),
        Index(columns=("opened_at",)),
        Index(columns=("sys_updated_on",)),
    ]


@register
class ServiceNowChangeTasks(ServiceNowTableAsset):
    """Change task records (children of change requests)."""

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
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("change_request",)),
        Index(columns=("state",)),
        Index(columns=("sys_updated_on",)),
    ]


@register
class ServiceNowProblems(ServiceNowTableAsset):
    """Problem management records."""

    name = "servicenow_problems"
    target_table = "servicenow_problems"
    table_name = "problem"

    columns = [
        Column("sys_id", "TEXT", nullable=False),
        Column("number", "TEXT"),
        Column("short_description", "TEXT"),
        Column("state", "TEXT"),
        Column("priority", "TEXT"),
        Column("category", "TEXT"),
        Column("assigned_to", "TEXT"),
        Column("assignment_group", "TEXT"),
        Column("opened_at", "TIMESTAMPTZ"),
        Column("closed_at", "TIMESTAMPTZ", nullable=True),
        Column("sys_updated_on", "TIMESTAMPTZ"),
    ]
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("state",)),
        Index(columns=("priority",)),
        Index(columns=("assignment_group",)),
        Index(columns=("sys_updated_on",)),
    ]


# ---------------------------------------------------------------------------
# User & organization directory
# ---------------------------------------------------------------------------


@register
class ServiceNowUsers(ServiceNowTableAsset):
    """User directory (sys_user)."""

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
    indexes = [
        Index(columns=("user_name",), unique=True),
        Index(columns=("email",)),
        Index(columns=("active",)),
        Index(columns=("sys_updated_on",)),
    ]


@register
class ServiceNowUserGroups(ServiceNowTableAsset):
    """User groups (sys_user_group)."""

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


@register
class ServiceNowDepartments(ServiceNowTableAsset):
    """Department directory (cmn_department)."""

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


@register
class ServiceNowLocations(ServiceNowTableAsset):
    """Site and office locations (cmn_location)."""

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


# ---------------------------------------------------------------------------
# CMDB & hardware
# ---------------------------------------------------------------------------


@register
class ServiceNowCmdbCIs(ServiceNowTableAsset):
    """CMDB configuration items (cmdb_ci)."""

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
    indexes = [
        Index(columns=("sys_class_name",)),
        Index(columns=("name",)),
        Index(columns=("assigned_to",)),
        Index(columns=("sys_updated_on",)),
    ]


@register
class ServiceNowHardwareAssets(ServiceNowTableAsset):
    """Hardware asset inventory (alm_hardware)."""

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
    indexes = [
        Index(columns=("asset_tag",)),
        Index(columns=("serial_number",)),
        Index(columns=("assigned_to",)),
        Index(columns=("sys_updated_on",)),
    ]


# ---------------------------------------------------------------------------
# Service catalog
# ---------------------------------------------------------------------------


@register
class ServiceNowCatalogItems(ServiceNowTableAsset):
    """Service catalog request items (sc_req_item)."""

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


@register
class ServiceNowCatalogRequests(ServiceNowTableAsset):
    """Service catalog requests (sc_request)."""

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
    indexes = [
        Index(columns=("number",), unique=True),
        Index(columns=("request_state",)),
        Index(columns=("requested_for",)),
        Index(columns=("sys_updated_on",)),
    ]


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------


@register
class ServiceNowChoices(ServiceNowTableAsset):
    """Dropdown decode table (sys_choice).

    Full-replaced each run — reference data with no reliable incremental sync.
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
