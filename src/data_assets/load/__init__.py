"""Load layer: DDL, temp tables, and promotion — all in loader.py."""

from data_assets.load.loader import (  # noqa: F401
    create_table,
    create_temp_table,
    drop_table,
    drop_temp_table,
    ensure_columns,
    promote,
    read_temp_table,
    temp_table_exists,
    temp_table_name,
    write_to_temp,
)
