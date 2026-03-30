"""Unit tests for checkpoint manager — acquire_or_takeover and heartbeat."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from data_assets.checkpoint.manager import LockError, acquire_or_takeover
from data_assets.core.identifiers import uuid7


def _mock_lock(
    asset_name="test_asset",
    run_id=None,
    locked_at=None,
    heartbeat_at=None,
    temp_table="old_temp",
    locked_by="worker-1",
):
    """Create a mock RunLock row."""
    lock = MagicMock()
    lock.asset_name = asset_name
    lock.run_id = run_id or uuid7()
    lock.locked_at = locked_at or datetime.now(UTC)
    lock.heartbeat_at = heartbeat_at or lock.locked_at
    lock.temp_table = temp_table
    lock.locked_by = locked_by
    return lock


@patch("data_assets.checkpoint.manager.Session")
def test_acquire_fresh_start(mock_session_cls):
    """No existing lock → fresh start, returns (None, None)."""
    session = MagicMock()
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
    session.execute.return_value.scalar_one_or_none.return_value = None

    inherited, abandoned = acquire_or_takeover(
        MagicMock(), "test_asset", uuid7(), "new_temp"
    )
    assert inherited is None
    assert abandoned is None
    session.add.assert_called_once()  # new lock created
    session.commit.assert_called_once()


@patch("data_assets.checkpoint.manager.Session")
def test_acquire_active_lock_raises(mock_session_cls):
    """Non-stale lock → LockError."""
    session = MagicMock()
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    # Lock is 5 minutes old with recent heartbeat — active
    now = datetime.now(UTC)
    lock = _mock_lock(locked_at=now - timedelta(minutes=5), heartbeat_at=now)
    session.execute.return_value.scalar_one_or_none.return_value = lock

    with pytest.raises(LockError, match="is locked by run"):
        acquire_or_takeover(MagicMock(), "test_asset", uuid7(), "new_temp")


@patch("data_assets.checkpoint.manager.Session")
def test_takeover_stale_heartbeat(mock_session_cls):
    """Heartbeat older than threshold → takeover."""
    session = MagicMock()
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    old_run_id = uuid7()
    now = datetime.now(UTC)
    lock = _mock_lock(
        run_id=old_run_id,
        locked_at=now - timedelta(minutes=30),
        heartbeat_at=now - timedelta(minutes=25),  # 25 min, > 20 min threshold
        temp_table="inherited_temp_tbl",
    )
    session.execute.return_value.scalar_one_or_none.return_value = lock

    inherited, abandoned = acquire_or_takeover(
        MagicMock(), "test_asset", uuid7(), "new_temp",
        stale_heartbeat_minutes=20,
    )
    assert inherited == "inherited_temp_tbl"
    assert abandoned == old_run_id
    session.delete.assert_called_once_with(lock)


@patch("data_assets.checkpoint.manager.Session")
def test_active_lock_just_under_threshold(mock_session_cls):
    """Lock with heartbeat 19 min ago (< 20 min threshold) should NOT trigger takeover."""
    session = MagicMock()
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    now = datetime.now(UTC)
    lock = _mock_lock(
        locked_at=now - timedelta(minutes=19),
        heartbeat_at=now - timedelta(minutes=19),
    )
    session.execute.return_value.scalar_one_or_none.return_value = lock

    with pytest.raises(LockError):
        acquire_or_takeover(MagicMock(), "test_asset", uuid7(), "new_temp",
                            stale_heartbeat_minutes=20)


@patch("data_assets.checkpoint.manager.Session")
def test_takeover_max_run_time(mock_session_cls):
    """Run exceeding max_run_hours → takeover even if heartbeat is recent."""
    session = MagicMock()
    mock_session_cls.return_value.__enter__ = MagicMock(return_value=session)
    mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)

    old_run_id = uuid7()
    now = datetime.now(UTC)
    lock = _mock_lock(
        run_id=old_run_id,
        locked_at=now - timedelta(hours=6),  # 6h, > 5h threshold
        heartbeat_at=now - timedelta(minutes=1),  # recent heartbeat
        temp_table="old_temp",
    )
    session.execute.return_value.scalar_one_or_none.return_value = lock

    inherited, abandoned = acquire_or_takeover(
        MagicMock(), "test_asset", uuid7(), "new_temp",
        max_run_hours=5,
    )
    assert inherited == "old_temp"
    assert abandoned == old_run_id
