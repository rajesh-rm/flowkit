"""Tests for data_assets.cli."""

from __future__ import annotations

import json

from data_assets.cli import main
from data_assets.core.asset import Asset
from data_assets.core.column import Column, Index
from data_assets.core.registry import register


def _register_stub():
    class _CliStub(Asset):
        name = "cli_stub"
        target_table = "cli_stub"
        description = "A test asset"
        columns = [Column("id", "text")]
        primary_key = ["id"]
        indexes = [Index(columns=["id"])]

    register(_CliStub)
    return _CliStub


def test_list_command(capsys):
    _register_stub()
    main(["list"])
    out = capsys.readouterr().out
    assert "cli_stub" in out


def test_list_json(capsys):
    _register_stub()
    main(["list", "--json"])
    out = capsys.readouterr().out
    data = json.loads(out)
    names = [r["name"] for r in data]
    assert "cli_stub" in names


def test_sync_command(tmp_path, capsys):
    _register_stub()
    main(["sync", "--output-dir", str(tmp_path)])
    out = capsys.readouterr().out
    assert "created" in out
    assert (tmp_path / "dag_cli_stub.py").exists()


def test_fingerprint_command(capsys):
    _register_stub()
    main(["fingerprint", "cli_stub"])
    out = capsys.readouterr().out.strip()
    assert len(out) == 16


def test_setup_systemd(tmp_path, capsys):
    main([
        "setup-systemd",
        "--output-dir", str(tmp_path),
        "--dag-dir", "/opt/airflow/dags/data_assets",
        "--venv-path", "/opt/airflow/venv",
        "--interval", "30",
    ])
    out = capsys.readouterr().out
    assert "Generated" in out
    assert (tmp_path / "data-assets-sync.service").exists()
    assert (tmp_path / "data-assets-sync.timer").exists()
    assert (tmp_path / "data-assets-setup.sh").exists()

    service = (tmp_path / "data-assets-sync.service").read_text()
    assert "data-assets sync" in service
    assert "User=airflow" in service
    timer = (tmp_path / "data-assets-sync.timer").read_text()
    assert "30min" in timer
    assert "RandomizedDelaySec" in timer

    setup = (tmp_path / "data-assets-setup.sh").read_text()
    assert "restorecon" in setup
    assert "install -m 644" in setup
    assert "systemctl enable --now" in setup
    # Instructions point to the setup script, no inline sudo commands
    assert "sudo bash" in out
    assert "data-assets-setup.sh" in out
