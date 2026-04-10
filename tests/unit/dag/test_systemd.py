"""Tests for data_assets.dag.systemd."""

from __future__ import annotations

from data_assets.dag.systemd import generate_systemd_units


def test_service_includes_toml_backup():
    service, _, _ = generate_systemd_units(
        dag_dir="/opt/airflow/dags/data_assets",
    )
    assert ".toml_backups" in service
    assert "dag_overrides" in service
    assert "cp " in service


def test_service_includes_corruption_guard():
    service, _, _ = generate_systemd_units(
        dag_dir="/opt/airflow/dags/data_assets",
    )
    assert "tomllib" in service
    assert "corrupt" in service
    assert "logger -p user.err" in service
    assert "exit 1" in service


def test_service_prunes_old_backups():
    service, _, _ = generate_systemd_units(
        dag_dir="/opt/airflow/dags/data_assets",
    )
    assert "-mtime +30 -delete" in service


def test_service_backup_4_per_day():
    """Backup uses hour/6 bucketing for 4 backups per day."""
    service, _, _ = generate_systemd_units(
        dag_dir="/opt/airflow/dags/data_assets",
    )
    # HOUR / 6 gives 4 buckets: 0, 1, 2, 3
    assert "HOUR / 6" in service


def test_service_skips_sync_on_corrupt_toml():
    """Corruption guard runs before pip install + sync."""
    service, _, _ = generate_systemd_units(
        dag_dir="/opt/airflow/dags/data_assets",
    )
    # The corruption check must appear before the pip install
    corrupt_pos = service.index("corrupt")
    pip_pos = service.index("pip install")
    assert corrupt_pos < pip_pos


def test_service_dag_dir_substituted():
    service, _, _ = generate_systemd_units(
        dag_dir="/custom/dags/path",
    )
    assert "/custom/dags/path/dag_overrides.toml" in service
    assert "/custom/dags/path/.toml_backups" in service
