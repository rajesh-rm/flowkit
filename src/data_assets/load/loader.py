"""Unified loader: DDL, temp tables, and promotion in one module.

Handles the full data loading lifecycle:
- Schema DDL: create tables, add columns, drop tables
- Temp tables: create, write, read, check existence, drop
- Promotion: move data from temp → main table via full_replace/upsert/append

All dialect-specific SQL is delegated to ``db.dialect``.
"""

from __future__ import annotations

import logging
import re
import uuid

import pandas as pd
from sqlalchemy import Text as SAText
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, SchemaContract
from data_assets.db.dialect import get_dialect
from data_assets.db.retry import db_retry

logger = logging.getLogger(__name__)

TEMP_SCHEMA = "temp_store"


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

def create_table(
    engine: Engine,
    schema: str,
    table_name: str,
    columns: list[Column],
    primary_key: list[str] | None = None,
    unlogged: bool = False,
) -> None:
    """CREATE TABLE from Column definitions (idempotent)."""
    insp = inspect(engine)
    if insp.has_table(table_name, schema=schema):
        return

    d = get_dialect(engine)
    pk_set = set(primary_key) if primary_key else set()
    adjusted_cols = d.adjust_pk_columns(columns, pk_set)

    col_defs = ", ".join(d.column_ddl(c) for c in adjusted_cols)
    pk_clause = ""
    if primary_key:
        pk_cols = ", ".join(d.qi(c) for c in primary_key)
        pk_clause = f", PRIMARY KEY ({pk_cols})"

    create_kw = d.create_table_kw(unlogged)
    ddl = f"{create_kw} {d.fqn(schema, table_name)} ({col_defs}{pk_clause})"
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Created table %s.%s", schema, table_name)


def ensure_columns(
    engine: Engine,
    schema: str,
    table_name: str,
    columns: list[Column],
    schema_contract: str | SchemaContract = "evolve",
) -> None:
    """Manage column differences between asset definition and table."""
    insp = inspect(engine)
    if not insp.has_table(table_name, schema=schema):
        return

    existing = {c["name"] for c in insp.get_columns(table_name, schema=schema)}
    new_cols = [c for c in columns if c.name not in existing]
    if not new_cols:
        return

    if schema_contract == "freeze":
        names = [c.name for c in new_cols]
        raise ValueError(
            f"Schema contract 'freeze' violated: new columns {names} "
            f"not in {schema}.{table_name}. Manually add them or change to 'evolve'."
        )

    if schema_contract == "discard":
        logger.info(
            "Schema contract 'discard': ignoring %d new columns for %s.%s",
            len(new_cols), schema, table_name,
        )
        return

    # Default: evolve — auto-add
    d = get_dialect(engine)
    with engine.begin() as conn:
        for col in new_cols:
            conn.execute(text(
                f"ALTER TABLE {d.fqn(schema, table_name)} ADD COLUMN {d.column_ddl(col)}"
            ))
            logger.info("Added column '%s' to %s.%s", col.name, schema, table_name)


def ensure_indexes(
    engine: Engine,
    schema: str,
    table_name: str,
    indexes: list[Index],
    columns: list[Column] | None = None,
) -> None:
    """Create declared indexes on the target table (idempotent).

    Uses CREATE INDEX IF NOT EXISTS so it is safe to call on every run.
    Each index is created in its own transaction so one failure does not
    block the others.

    If a UNIQUE index fails due to duplicate values (IntegrityError),
    falls back to a non-unique index and logs a warning.
    """
    d = get_dialect(engine)
    column_types = {c.name: c.sa_type for c in columns} if columns else None
    for idx in indexes:
        ddl = d.create_index_ddl(schema, table_name, idx, column_types=column_types)
        try:
            with engine.begin() as conn:
                conn.execute(text(ddl))
            logger.debug("Ensured index on %s.%s", schema, table_name)
        except IntegrityError:
            if not idx.unique:
                raise
            col_names = ", ".join(idx.columns)
            logger.warning(
                "UNIQUE index on %s.%s (%s) failed due to duplicate values. "
                "Falling back to non-unique index. "
                "Investigate duplicate data in column(s): %s",
                schema, table_name, col_names, col_names,
            )
            fallback_idx = Index(
                columns=idx.columns, unique=False, method=idx.method,
                where=idx.where, include=idx.include,
            )
            fallback_ddl = d.create_index_ddl(
                schema, table_name, fallback_idx, column_types=column_types,
            )
            with engine.begin() as conn:
                conn.execute(text(fallback_ddl))
            logger.info(
                "Created non-unique fallback index on %s.%s (%s)",
                schema, table_name, col_names,
            )


def _nullify_empty_strings_for_unique_indexes(
    engine: Engine,
    schema: str,
    table_name: str,
    indexes: list[Index],
    columns: list[Column],
) -> None:
    """Replace empty strings with NULL in Text columns covered by unique indexes.

    PostgreSQL and MariaDB both treat ``''`` as a regular value that violates
    UNIQUE constraints, but allow multiple NULLs.  Running this before
    ``ensure_indexes()`` prevents UniqueViolation on columns where the source
    system sends empty strings for missing values.
    """
    text_col_names = {c.name for c in columns if isinstance(c.sa_type, SAText)}
    unique_text_cols: set[str] = set()
    for idx in indexes:
        if idx.unique:
            for col_name in idx.columns:
                if col_name in text_col_names:
                    unique_text_cols.add(col_name)

    if not unique_text_cols:
        return

    d = get_dialect(engine)
    fqn = d.fqn(schema, table_name)
    with engine.begin() as conn:
        for col_name in sorted(unique_text_cols):
            qcol = d.qi(col_name)
            result = conn.execute(text(
                f"UPDATE {fqn} SET {qcol} = NULL WHERE {qcol} = ''"
            ))
            if result.rowcount > 0:
                logger.info(
                    "Nullified %d empty string(s) in %s.%s.%s for unique index",
                    result.rowcount, schema, table_name, col_name,
                )


def _warn_unique_index_violations(
    engine: Engine,
    schema: str,
    table_name: str,
    indexes: list[Index],
) -> None:
    """Log diagnostics for columns covered by unique indexes.

    Runs after nullification and before ``ensure_indexes()`` so the warnings
    reflect the actual state that will be indexed.  Reports duplicate non-NULL
    values that will prevent a unique index from being created.
    """
    unique_indexes = [idx for idx in indexes if idx.unique]
    if not unique_indexes:
        return

    d = get_dialect(engine)
    fqn = d.fqn(schema, table_name)
    with engine.begin() as conn:
        for idx in unique_indexes:
            cols_sql = ", ".join(d.qi(c) for c in idx.columns)
            col_label = ", ".join(idx.columns)

            not_null_conditions = " AND ".join(
                f"{d.qi(c)} IS NOT NULL" for c in idx.columns
            )
            dup_row = conn.execute(text(
                f"SELECT {cols_sql}, COUNT(*) AS cnt "
                f"FROM {fqn} "
                f"WHERE {not_null_conditions} "
                f"GROUP BY {cols_sql} "
                f"HAVING COUNT(*) > 1 "
                f"ORDER BY COUNT(*) DESC "
                f"LIMIT 3"
            )).fetchall()
            if dup_row:
                total_groups = len(dup_row)
                samples = "; ".join(
                    f"{{{', '.join(f'{c}={row._mapping[c]!r}' for c in idx.columns)}}} "
                    f"({row._mapping['cnt']} rows)"
                    for row in dup_row
                )
                logger.warning(
                    "Unique index column(s) (%s) in %s.%s has duplicate values "
                    "(top %d groups): %s. "
                    "The unique index will fall back to non-unique.",
                    col_label, schema, table_name, total_groups, samples,
                )


def drop_table(engine: Engine, schema: str, table_name: str) -> None:
    """Drop a table if it exists."""
    d = get_dialect(engine)
    with engine.begin() as conn:
        conn.execute(text(d.drop_table_ddl(schema, table_name)))


# ---------------------------------------------------------------------------
# Temp table operations
# ---------------------------------------------------------------------------

def temp_table_name(asset_name: str, run_id: uuid.UUID) -> str:
    """Generate a deterministic temp table name for a run."""
    short_id = str(run_id).replace("-", "")[:12]
    return f"{asset_name}_{short_id}"


def create_temp_table(
    engine: Engine,
    asset_name: str,
    run_id: uuid.UUID,
    columns: list[Column],
) -> str:
    """Create a temp table in temp_store. Returns the table name."""
    tname = temp_table_name(asset_name, run_id)
    create_table(engine, TEMP_SCHEMA, tname, columns, primary_key=None, unlogged=True)
    logger.debug("Created temp table %s.%s", TEMP_SCHEMA, tname)
    return tname


# Matches ISO 8601 ("2025-12-01T09:00:00Z") and ServiceNow ("2025-12-01 09:00:00")
_DATETIME_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")


def _coerce_datetime_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime-like string columns to proper datetime objects in place.

    Detects columns whose first non-empty sample matches a datetime pattern
    (ISO 8601 or space-separated) and converts them via ``pd.to_datetime``.
    Empty strings are replaced with ``None`` before conversion so they become
    ``NaT`` rather than causing database type errors.
    """
    for col in df.columns:
        if df[col].dtype not in ("object", "str", "string"):
            continue
        non_empty = df[col].loc[df[col].notna() & (df[col] != "")]
        if non_empty.empty:
            continue
        sample = non_empty.iloc[0]
        if isinstance(sample, str) and _DATETIME_RE.match(sample):
            try:
                df[col] = df[col].replace("", None)
                df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
            except Exception:
                pass  # not a datetime column after all
    return df


@db_retry()
def write_to_temp(
    engine: Engine,
    table_name: str,
    df: pd.DataFrame,
    *,
    sensitive_columns: list[str] | None = None,
    tokenization_client=None,
) -> int:
    """Append a DataFrame to the temp table. Returns rows written.

    When *sensitive_columns* is non-empty, each listed column is tokenized
    via the external service before any DB write — plaintext values never
    reach temp_store. Tokenization failures raise ``TokenizationError`` and
    abort the run; ``@db_retry`` does not retry on it (only on DB-transient
    errors). Pass *tokenization_client* to inject a custom client (mainly
    for tests); otherwise the lazily-built default client is used.
    """
    if df.empty:
        return 0
    rows = len(df)

    df = df.copy()
    _coerce_datetime_strings(df)

    # Dialect-specific adjustments (e.g., MariaDB strips timezone info).
    d = get_dialect(engine)
    df = d.prepare_dataframe(df)

    if sensitive_columns:
        # Local imports keep this dependency lazy: assets without sensitive
        # data never need the tokenization stack to import.
        from data_assets.extract.tokenization_client import get_default_client
        from data_assets.load.tokenization import apply_tokenization
        client = tokenization_client or get_default_client()
        df = apply_tokenization(df, sensitive_columns, client)

    df.to_sql(
        table_name, engine, schema=TEMP_SCHEMA,
        if_exists="append", index=False, method="multi",
        chunksize=1000,
    )
    logger.debug("Wrote %d rows to %s.%s", rows, TEMP_SCHEMA, table_name)
    return rows


def read_temp_table(engine: Engine, table_name: str) -> pd.DataFrame:
    """Read the entire temp table into a DataFrame."""
    d = get_dialect(engine)
    return pd.read_sql(f"SELECT * FROM {d.fqn(TEMP_SCHEMA, table_name)}", engine)


def drop_temp_table(engine: Engine, table_name: str) -> None:
    """Drop a temp table after successful promotion."""
    drop_table(engine, TEMP_SCHEMA, table_name)


def temp_table_exists(engine: Engine, table_name: str) -> bool:
    """Check if a temp table exists."""
    return inspect(engine).has_table(table_name, schema=TEMP_SCHEMA)


# ---------------------------------------------------------------------------
# Promotion: temp → main table
# ---------------------------------------------------------------------------

@db_retry()
def promote(
    engine: Engine,
    temp_table: str,
    target_schema: str,
    target_table: str,
    columns: list[Column],
    primary_key: list[str],
    load_strategy: LoadStrategy,
    schema_contract: str = "evolve",
    indexes: list[Index] | None = None,
) -> int:
    """Promote data from temp table to main table in a single transaction.

    Ensures target table exists (creates if missing, manages columns per schema_contract).
    Creates declared indexes after promotion (idempotent).
    Returns number of rows loaded.
    """
    create_table(engine, target_schema, target_table, columns, primary_key)
    ensure_columns(engine, target_schema, target_table, columns, schema_contract)

    column_names = [c.name for c in columns]
    d = get_dialect(engine)
    promoter = _PROMOTERS[load_strategy.value]

    with engine.begin() as conn:
        # Bound the entire promote txn so a hung statement (lock contention on
        # temp or main, stalled INSERT) fails fast instead of holding the
        # Airflow slot for max_run_hours. Covers dedup and all three promoters.
        d.set_query_timeout(conn, 300)

        # Dedup: remove duplicate PK rows within the temp table before promotion.
        # Resumed or inherited temp tables can contain duplicates from retries.
        if primary_key:
            removed = d.dedup_temp_table(conn, TEMP_SCHEMA, temp_table, primary_key)
            if removed > 0:
                logger.warning(
                    "Removed %d duplicate rows from temp table before promotion",
                    removed,
                )

        rows_loaded = promoter(conn, d, TEMP_SCHEMA, temp_table, target_schema,
                               target_table, primary_key, column_names)

    if indexes:
        _nullify_empty_strings_for_unique_indexes(
            engine, target_schema, target_table, indexes, columns,
        )
        _warn_unique_index_violations(
            engine, target_schema, target_table, indexes,
        )
        ensure_indexes(engine, target_schema, target_table, indexes, columns)

    logger.info("Promoted %d rows to %s.%s via %s",
                rows_loaded, target_schema, target_table, load_strategy.value)
    return rows_loaded


def _promote_full_replace(conn, d, temp_schema, temp_table, main_schema, main_table,
                          primary_key, column_names) -> int:
    """Empty main table, then INSERT...SELECT from temp — atomic per dialect."""
    d.delete_all_rows(conn, main_schema, main_table)
    cols = ", ".join(d.qi(c) for c in column_names)
    result = conn.execute(text(
        f"INSERT INTO {d.fqn(main_schema, main_table)} ({cols}) "
        f"SELECT {cols} FROM {d.fqn(temp_schema, temp_table)}"
    ))
    return result.rowcount


def _promote_upsert(conn, d, temp_schema, temp_table, main_schema, main_table,
                    primary_key, column_names) -> int:
    """INSERT with conflict handling from temp, using dialect-specific SQL."""
    sql = d.upsert_sql(main_schema, main_table, temp_schema, temp_table,
                       primary_key, column_names)
    return conn.execute(text(sql)).rowcount


def _promote_append(conn, d, temp_schema, temp_table, main_schema, main_table,
                    primary_key, column_names) -> int:
    """INSERT...SELECT from temp (no conflict handling)."""
    cols = ", ".join(d.qi(c) for c in column_names)
    return conn.execute(text(
        f"INSERT INTO {d.fqn(main_schema, main_table)} ({cols}) "
        f"SELECT {cols} FROM {d.fqn(temp_schema, temp_table)}"
    )).rowcount


_PROMOTERS = {
    "full_replace": _promote_full_replace,
    "upsert": _promote_upsert,
    "append": _promote_append,
}
