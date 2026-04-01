"""Integration tests for checkpoint manager against real Postgres."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from data_assets.checkpoint.manager import (
    LockError,
    acquire_or_takeover,
    clear_checkpoints,
    get_checkpoints,
    release_lock,
    save_checkpoint,
    update_lock_temp_table,
)
from data_assets.core.identifiers import uuid7


@pytest.mark.integration
class TestAcquireOrTakeover:
    def test_fresh_start(self, clean_db):
        run_id = uuid7()
        inherited, abandoned = acquire_or_takeover(
            clean_db, "test_asset", run_id, "temp_tbl"
        )
        assert inherited is None
        assert abandoned is None

    def test_active_lock_raises(self, clean_db):
        acquire_or_takeover(clean_db, "test_asset", uuid7(), "temp1")
        with pytest.raises(LockError, match="is locked"):
            acquire_or_takeover(clean_db, "test_asset", uuid7(), "temp2")

    def test_takeover_stale_heartbeat(self, clean_db):
        old_id = uuid7()
        acquire_or_takeover(clean_db, "test_asset", old_id, "old_temp")

        # Manually age the heartbeat
        from sqlalchemy import text
        with clean_db.begin() as conn:
            conn.execute(text(
                "UPDATE data_ops.run_locks SET heartbeat_at = :ts WHERE asset_name = 'test_asset'"
            ), {"ts": datetime.now(UTC) - timedelta(minutes=30)})

        new_id = uuid7()
        inherited, abandoned = acquire_or_takeover(
            clean_db, "test_asset", new_id, "new_temp",
            stale_heartbeat_minutes=20,
        )
        assert inherited == "old_temp"
        assert abandoned == old_id

    def test_release_lock(self, clean_db):
        acquire_or_takeover(clean_db, "test_asset", uuid7(), "temp")
        release_lock(clean_db, "test_asset")
        # Can acquire again after release
        acquire_or_takeover(clean_db, "test_asset", uuid7(), "temp2")

    def test_update_lock_temp_table(self, clean_db):
        acquire_or_takeover(clean_db, "test_asset", uuid7(), "old_temp")
        update_lock_temp_table(clean_db, "test_asset", "new_temp")

        from sqlalchemy import select
        from sqlalchemy.orm import Session
        from data_assets.db.models import RunLock

        with Session(clean_db) as session:
            lock = session.execute(
                select(RunLock).where(RunLock.asset_name == "test_asset")
            ).scalar_one()
            assert lock.temp_table == "new_temp"


@pytest.mark.integration
class TestCheckpointCRUD:
    def test_save_and_get(self, clean_db):
        run_id = uuid7()
        acquire_or_takeover(clean_db, "test_asset", run_id, "temp")

        save_checkpoint(
            clean_db, run_id=run_id, asset_name="test_asset",
            worker_id="main", checkpoint_type="page",
            checkpoint_value={"cursor": "abc"}, rows_so_far=10,
        )

        cps = get_checkpoints(clean_db, "test_asset")
        assert len(cps) == 1
        assert cps[0].worker_id == "main"
        assert cps[0].checkpoint_value == {"cursor": "abc"}
        assert cps[0].rows_so_far == 10

    def test_save_updates_existing(self, clean_db):
        run_id = uuid7()
        acquire_or_takeover(clean_db, "test_asset", run_id, "temp")

        save_checkpoint(
            clean_db, run_id=run_id, asset_name="test_asset",
            worker_id="main", checkpoint_type="page",
            checkpoint_value={"cursor": "abc"}, rows_so_far=10,
        )
        save_checkpoint(
            clean_db, run_id=run_id, asset_name="test_asset",
            worker_id="main", checkpoint_type="page",
            checkpoint_value={"cursor": "def"}, rows_so_far=20,
        )

        cps = get_checkpoints(clean_db, "test_asset")
        assert len(cps) == 1  # updated, not duplicated
        assert cps[0].checkpoint_value == {"cursor": "def"}
        assert cps[0].rows_so_far == 20

    def test_save_refreshes_heartbeat(self, clean_db):
        run_id = uuid7()
        acquire_or_takeover(clean_db, "test_asset", run_id, "temp")

        # Age the heartbeat
        from sqlalchemy import select, text
        from sqlalchemy.orm import Session
        from data_assets.db.models import RunLock

        with clean_db.begin() as conn:
            conn.execute(text(
                "UPDATE data_ops.run_locks SET heartbeat_at = :ts WHERE asset_name = 'test_asset'"
            ), {"ts": datetime.now(UTC) - timedelta(minutes=10)})

        save_checkpoint(
            clean_db, run_id=run_id, asset_name="test_asset",
            worker_id="main", checkpoint_type="page",
            checkpoint_value={}, rows_so_far=0,
        )

        with Session(clean_db) as session:
            lock = session.execute(
                select(RunLock).where(RunLock.asset_name == "test_asset")
            ).scalar_one()
            age = datetime.now(UTC) - lock.heartbeat_at.replace(tzinfo=UTC)
            assert age.total_seconds() < 5  # heartbeat was refreshed

    def test_clear_checkpoints(self, clean_db):
        run_id = uuid7()
        acquire_or_takeover(clean_db, "test_asset", run_id, "temp")

        save_checkpoint(
            clean_db, run_id=run_id, asset_name="test_asset",
            worker_id="w0", checkpoint_type="page",
            checkpoint_value={}, rows_so_far=5,
        )
        save_checkpoint(
            clean_db, run_id=run_id, asset_name="test_asset",
            worker_id="w1", checkpoint_type="page",
            checkpoint_value={}, rows_so_far=3,
        )

        clear_checkpoints(clean_db, "test_asset")
        assert len(get_checkpoints(clean_db, "test_asset")) == 0

    def test_multiple_workers(self, clean_db):
        run_id = uuid7()
        acquire_or_takeover(clean_db, "test_asset", run_id, "temp")

        for i in range(3):
            save_checkpoint(
                clean_db, run_id=run_id, asset_name="test_asset",
                worker_id=f"worker_{i}", checkpoint_type="entity",
                checkpoint_value={"entity": f"e{i}"}, rows_so_far=i * 10,
            )

        cps = get_checkpoints(clean_db, "test_asset")
        assert len(cps) == 3
        worker_ids = {cp.worker_id for cp in cps}
        assert worker_ids == {"worker_0", "worker_1", "worker_2"}

    def test_save_checkpoint_rejects_preempted_worker(self, clean_db):
        """A worker that no longer owns the lock cannot save checkpoints."""
        old_run = uuid7()
        new_run = uuid7()

        # Worker A acquires lock
        acquire_or_takeover(clean_db, "test_asset", old_run, "temp_old")

        # Worker B takes over (simulated by releasing and re-acquiring)
        release_lock(clean_db, "test_asset")
        acquire_or_takeover(clean_db, "test_asset", new_run, "temp_new")

        # Worker A tries to save a checkpoint — should be rejected
        with pytest.raises(RuntimeError, match="no longer owns the lock"):
            save_checkpoint(
                clean_db, run_id=old_run, asset_name="test_asset",
                worker_id="main", checkpoint_type="page",
                checkpoint_value={"page": 5}, rows_so_far=100,
            )
