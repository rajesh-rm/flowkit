"""End-to-end tokenization: write_to_temp → promote against real DB.

Drives the full load path with a mocked tokenization endpoint and asserts:
  1. Sensitive plaintext never lands in the temp table.
  2. The promoted table contains the expected tokenized values.
  3. UPSERT-with-sensitive-PK is idempotent across re-runs (relies on the
     determinism of the mocked endpoint, mirroring the production
     requirement).
  4. NULLs in sensitive columns are preserved across the pipeline.
"""

from __future__ import annotations

import uuid

import httpx
import pandas as pd
import pytest
import respx
from sqlalchemy import Integer, Text

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy
from data_assets.extract.tokenization_client import TokenizationClient
from data_assets.load.loader import (
    create_temp_table,
    drop_temp_table,
    promote,
    read_temp_table,
    write_to_temp,
)

URL = "https://tokenizer.test/v1/tokenize"

USER_COLS = [
    Column("user_id", Text(), nullable=False, sensitive=True),
    Column("display_name", Text()),
    Column("email", Text(), sensitive=True),
    Column("score", Integer()),
]
SENSITIVE = ["user_id", "email"]
PK = ["user_id"]
INDEXES = [Index(columns=("display_name",))]


@pytest.fixture
def deterministic_client():
    """A real TokenizationClient pointed at a respx-mocked endpoint with a
    deterministic ``f"tok_{v}"`` transform. Same input → same token across
    calls — required for upsert convergence on a sensitive PK."""
    c = TokenizationClient(
        URL, "test-key", base_delay=0.0, max_delay=0.0, max_attempts=2,
    )
    yield c
    c.close()


def _deterministic_route():
    return respx.post(URL).mock(
        side_effect=lambda req: httpx.Response(
            200,
            json={"tokens": [f"tok_{v}" for v in _read_values(req)]},
        ),
    )


@pytest.mark.integration
class TestTokenizationEndToEnd:

    @respx.mock
    def test_temp_table_never_sees_plaintext(
        self, clean_db, deterministic_client,
    ):
        _deterministic_route()
        run_id = uuid.uuid4()
        tname = create_temp_table(clean_db, "users", run_id, USER_COLS)
        try:
            df = pd.DataFrame({
                "user_id": ["alice", "bob", "alice"],
                "display_name": ["Alice A.", "Bob B.", "Alice A."],
                "email": ["a@x.com", "b@x.com", "a@x.com"],
                "score": [10, 20, 30],
            })
            write_to_temp(
                clean_db, tname, df,
                sensitive_columns=SENSITIVE,
                tokenization_client=deterministic_client,
            )

            stored = read_temp_table(clean_db, tname)
            assert set(stored["user_id"]) == {"tok_alice", "tok_bob"}
            assert set(stored["email"]) == {"tok_a@x.com", "tok_b@x.com"}
            # Non-sensitive columns untouched.
            assert set(stored["display_name"]) == {"Alice A.", "Bob B."}
            # Plaintext PII does not appear anywhere.
            assert "alice" not in set(stored["user_id"])
            assert "a@x.com" not in set(stored["email"])
        finally:
            drop_temp_table(clean_db, tname)

    @respx.mock
    def test_full_pipeline_promotes_tokenized_values(
        self, clean_db, deterministic_client,
    ):
        _deterministic_route()
        run_id = uuid.uuid4()
        tname = create_temp_table(clean_db, "users", run_id, USER_COLS)

        df = pd.DataFrame({
            "user_id": ["alice", "bob"],
            "display_name": ["Alice A.", "Bob B."],
            "email": ["a@x.com", "b@x.com"],
            "score": [10, 20],
        })
        write_to_temp(
            clean_db, tname, df,
            sensitive_columns=SENSITIVE,
            tokenization_client=deterministic_client,
        )
        promote(
            engine=clean_db, temp_table=tname,
            target_schema="raw", target_table="users_e2e",
            columns=USER_COLS, primary_key=PK,
            load_strategy=LoadStrategy.UPSERT,
            indexes=INDEXES,
        )
        drop_temp_table(clean_db, tname)

        result = pd.read_sql(
            "SELECT user_id, display_name, email FROM raw.users_e2e ORDER BY display_name",
            clean_db,
        )
        assert result["user_id"].tolist() == ["tok_alice", "tok_bob"]
        assert result["email"].tolist() == ["tok_a@x.com", "tok_b@x.com"]

    @respx.mock
    def test_upsert_with_sensitive_pk_is_idempotent(
        self, clean_db, deterministic_client,
    ):
        # Second run with same input must converge — depends on token
        # determinism (production tokenization service must satisfy this).
        _deterministic_route()
        df = pd.DataFrame({
            "user_id": ["alice", "bob"],
            "display_name": ["Alice", "Bob"],
            "email": ["a@x.com", "b@x.com"],
            "score": [10, 20],
        })

        for _ in range(2):
            run_id = uuid.uuid4()
            tname = create_temp_table(clean_db, "users", run_id, USER_COLS)
            write_to_temp(
                clean_db, tname, df,
                sensitive_columns=SENSITIVE,
                tokenization_client=deterministic_client,
            )
            promote(
                engine=clean_db, temp_table=tname,
                target_schema="raw", target_table="users_idem",
                columns=USER_COLS, primary_key=PK,
                load_strategy=LoadStrategy.UPSERT,
                indexes=INDEXES,
            )
            drop_temp_table(clean_db, tname)

        count = pd.read_sql(
            "SELECT COUNT(*) AS n FROM raw.users_idem", clean_db,
        )["n"].iloc[0]
        assert int(count) == 2  # Two unique users, no duplicates.

    @respx.mock
    def test_nulls_in_sensitive_column_preserved(
        self, clean_db, deterministic_client,
    ):
        _deterministic_route()
        run_id = uuid.uuid4()
        tname = create_temp_table(clean_db, "users", run_id, USER_COLS)
        try:
            df = pd.DataFrame({
                "user_id": ["alice", "bob"],
                "display_name": ["Alice", "Bob"],
                "email": ["a@x.com", None],  # NULL in sensitive column.
                "score": [10, 20],
            })
            write_to_temp(
                clean_db, tname, df,
                sensitive_columns=SENSITIVE,
                tokenization_client=deterministic_client,
            )
            stored = read_temp_table(clean_db, tname)
            stored = stored.sort_values("user_id").reset_index(drop=True)
            assert stored["email"].iloc[0] == "tok_a@x.com"
            assert pd.isna(stored["email"].iloc[1])
        finally:
            drop_temp_table(clean_db, tname)

    @respx.mock
    def test_api_failure_aborts_before_db_write(
        self, clean_db, deterministic_client,
    ):
        respx.post(URL).mock(return_value=httpx.Response(500, text="dead"))
        run_id = uuid.uuid4()
        tname = create_temp_table(clean_db, "users", run_id, USER_COLS)
        try:
            df = pd.DataFrame({
                "user_id": ["alice"],
                "display_name": ["Alice"],
                "email": ["a@x.com"],
                "score": [10],
            })
            from data_assets.extract.tokenization_client import TokenizationError
            with pytest.raises(TokenizationError):
                write_to_temp(
                    clean_db, tname, df,
                    sensitive_columns=SENSITIVE,
                    tokenization_client=deterministic_client,
                )

            # No rows reached the temp table.
            stored = read_temp_table(clean_db, tname)
            assert len(stored) == 0
        finally:
            drop_temp_table(clean_db, tname)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_values(req: httpx.Request) -> list[str]:
    import json
    return json.loads(req.read())["values"]
