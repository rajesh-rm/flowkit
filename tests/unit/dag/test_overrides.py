"""Tests for data_assets.dag.overrides."""

from __future__ import annotations

import pytest

from data_assets.core.asset import Asset
from data_assets.core.column import Column, Index
from data_assets.core.enums import RunMode
from data_assets.dag.overrides import SCHEDULE_BY_MODE, load_overrides, merge_config


class _StubAsset(Asset):
    name = "stub_ov"
    target_table = "stub_ov"
    default_run_mode = RunMode.FULL
    columns = [Column("id", "text")]
    primary_key = ["id"]
    indexes = [Index(columns=["id"])]


class _StubWithDagConfig(Asset):
    name = "stub_dc"
    target_table = "stub_dc"
    default_run_mode = RunMode.FORWARD
    columns = [Column("id", "text")]
    primary_key = ["id"]
    indexes = [Index(columns=["id"])]
    dag_config = {"schedule": "*/30 * * * *", "retries": 5}


def test_load_missing_file(tmp_path):
    result = load_overrides(tmp_path)
    assert result == {}


def test_load_valid_toml(tmp_path):
    toml_file = tmp_path / "dag_overrides.toml"
    toml_file.write_text('[stub_ov]\nschedule = "0 3 * * *"\nretries = 10\n')
    result = load_overrides(tmp_path)
    assert result["stub_ov"]["schedule"] == "0 3 * * *"
    assert result["stub_ov"]["retries"] == 10


def test_merge_defaults_only():
    config = merge_config(_StubAsset, {})
    assert config["schedule"] == SCHEDULE_BY_MODE["full"]
    assert config["retries"] == 3
    assert config["owner"] == "data-engineering"
    assert config["run_mode"] == "full"


def test_merge_dag_config_overrides_defaults():
    config = merge_config(_StubWithDagConfig, {})
    assert config["schedule"] == "*/30 * * * *"
    assert config["retries"] == 5
    assert config["run_mode"] == "forward"


def test_merge_admin_overrides_win():
    overrides = {"stub_dc": {"schedule": "@daily", "retries": 1}}
    config = merge_config(_StubWithDagConfig, overrides)
    # Admin overrides win over dag_config
    assert config["schedule"] == "@daily"
    assert config["retries"] == 1


def test_schedule_from_run_mode():
    for mode_str, expected in SCHEDULE_BY_MODE.items():
        mode = RunMode(mode_str)

        class _Dynamic(Asset):
            name = "dyn"
            target_table = "dyn"
            default_run_mode = mode
            columns = [Column("id", "text")]
            primary_key = ["id"]
            indexes = [Index(columns=["id"])]

        config = merge_config(_Dynamic, {})
        assert config["schedule"] == expected, f"Failed for mode={mode_str}"


def test_invalid_toml_raises_with_path(tmp_path):
    toml_file = tmp_path / "dag_overrides.toml"
    toml_file.write_text("[broken\n")  # invalid TOML
    with pytest.raises(ValueError, match=str(tmp_path)):
        load_overrides(tmp_path)


def test_orgs_must_be_list_of_dicts():
    overrides = {"stub_ov": {"orgs": "not-a-list"}}
    with pytest.raises(ValueError, match="must be a list"):
        merge_config(_StubAsset, overrides)


def test_orgs_entries_require_org_key():
    overrides = {"stub_ov": {"orgs": [{"installation_id": "123"}]}}
    with pytest.raises(ValueError, match="missing required key 'org'"):
        merge_config(_StubAsset, overrides)
