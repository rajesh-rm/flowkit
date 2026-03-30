"""ServiceNow asset definitions.

Assets:
    - servicenow_incidents: Incident records from the incident table
    - servicenow_changes: Change request records from the change_request table
    - servicenow_cmdb_items: Configuration items from the cmdb_ci table

Connection: servicenow_default (http)
API: ServiceNow Table API (/api/now/table/{table_name})
"""
