"""Integration tests for run_tracker against real Postgres."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from data_assets.core.identifiers import uuid7
from data_assets.db.models import AssetRegistry, CoverageTracker, RunHistory
from data_assets.observability.run_tracker import (
    get_coverage,
    record_run_failure,
    record_run_start,
    record_run_success,
    update_coverage,
    update_last_success,
)


@pytest.mark.integration
class TestRunHistory:
    def test_record_start_and_success(self, clean_db):
        run_id = uuid7()
        record_run_start(clean_db, run_id, "test_asset", "full")

        with Session(clean_db) as session:
            row = session.execute(
                select(RunHistory).where(RunHistory.run_id == run_id)
            ).scalar_one()
            assert row.status == "running"
            assert row.asset_name == "test_asset"

        record_run_success(clean_db, run_id, rows_extracted=100, rows_loaded=95,
                           metadata={"api_calls": 5})

        with Session(clean_db) as session:
            row = session.execute(
                select(RunHistory).where(RunHistory.run_id == run_id)
            ).scalar_one()
            assert row.status == "success"
            assert row.rows_extracted == 100
            assert row.rows_loaded == 95
            assert row.completed_at is not None

    def test_record_failure(self, clean_db):
        run_id = uuid7()
        record_run_start(clean_db, run_id, "test_asset", "full")
        record_run_failure(clean_db, run_id, "Something broke")

        with Session(clean_db) as session:
            row = session.execute(
                select(RunHistory).where(RunHistory.run_id == run_id)
            ).scalar_one()
            assert row.status == "failed"
            assert row.error_message == "Something broke"

    def test_airflow_run_id_stored(self, clean_db):
        run_id = uuid7()
        record_run_start(clean_db, run_id, "test_asset", "full",
                         airflow_run_id="manual__2025-01-01")

        with Session(clean_db) as session:
            row = session.execute(
                select(RunHistory).where(RunHistory.run_id == run_id)
            ).scalar_one()
            assert row.airflow_run_id == "manual__2025-01-01"


@pytest.mark.integration
class TestCoverageTracker:
    def test_update_and_get(self, clean_db):
        ts = datetime(2025, 6, 15, 12, 0, tzinfo=UTC)
        update_coverage(clean_db, "test_asset", forward_watermark=ts)

        coverage = get_coverage(clean_db, "test_asset")
        assert coverage is not None
        assert coverage.forward_watermark.replace(tzinfo=UTC) == ts
        assert coverage.backward_watermark is None

    def test_upsert_updates_existing(self, clean_db):
        ts1 = datetime(2025, 6, 1, tzinfo=UTC)
        ts2 = datetime(2025, 6, 15, tzinfo=UTC)

        update_coverage(clean_db, "test_asset", forward_watermark=ts1)
        update_coverage(clean_db, "test_asset", forward_watermark=ts2)

        coverage = get_coverage(clean_db, "test_asset")
        assert coverage.forward_watermark.replace(tzinfo=UTC) == ts2

    def test_get_returns_none_for_unknown(self, clean_db):
        assert get_coverage(clean_db, "nonexistent") is None

    def test_backward_watermark(self, clean_db):
        ts = datetime(2024, 1, 1, tzinfo=UTC)
        update_coverage(clean_db, "test_asset", backward_watermark=ts)

        coverage = get_coverage(clean_db, "test_asset")
        assert coverage.backward_watermark.replace(tzinfo=UTC) == ts


@pytest.mark.integration
class TestUpdateLastSuccess:
    def test_updates_registry(self, clean_db):
        # Seed the registry
        with Session(clean_db) as session:
            session.add(AssetRegistry(
                asset_name="test_asset", asset_type="api",
                target_schema="raw", target_table="test",
                load_strategy="upsert",
            ))
            session.commit()

        update_last_success(clean_db, "test_asset")

        with Session(clean_db) as session:
            row = session.execute(
                select(AssetRegistry).where(AssetRegistry.asset_name == "test_asset")
            ).scalar_one()
            assert row.last_success_at is not None
