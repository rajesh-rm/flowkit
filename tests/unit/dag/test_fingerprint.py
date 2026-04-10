"""Tests for data_assets.dag.fingerprint."""

from __future__ import annotations

import re

from data_assets.core.asset import Asset
from data_assets.core.column import Column, Index
from data_assets.core.enums import RunMode
from data_assets.dag.fingerprint import compute_fingerprint
from sqlalchemy import Text


class _StubAsset(Asset):
    name = "stub_fp"
    target_table = "stub_fp"
    columns = [Column("id", Text())]
    primary_key = ["id"]
    indexes = [Index(columns=["id"])]


class _StubAssetAlt(Asset):
    name = "stub_fp_alt"
    target_table = "stub_fp_alt"
    columns = [Column("id", Text())]
    primary_key = ["id"]
    indexes = [Index(columns=["id"])]
    dag_config = {"schedule": "@hourly"}


def test_deterministic():
    a = compute_fingerprint(_StubAsset)
    b = compute_fingerprint(_StubAsset)
    assert a == b


def test_format():
    fp = compute_fingerprint(_StubAsset)
    assert len(fp) == 16
    assert re.fullmatch(r"[0-9a-f]{16}", fp)


def test_changes_on_dag_config():
    fp1 = compute_fingerprint(_StubAsset)
    fp2 = compute_fingerprint(_StubAssetAlt)
    assert fp1 != fp2


def test_changes_on_run_mode():
    class _FullMode(Asset):
        name = "stub_mode"
        target_table = "stub_mode"
        default_run_mode = RunMode.FULL
        columns = [Column("id", Text())]
        primary_key = ["id"]
        indexes = [Index(columns=["id"])]

    class _ForwardMode(Asset):
        name = "stub_mode"
        target_table = "stub_mode"
        default_run_mode = RunMode.FORWARD
        columns = [Column("id", Text())]
        primary_key = ["id"]
        indexes = [Index(columns=["id"])]

    assert compute_fingerprint(_FullMode) != compute_fingerprint(_ForwardMode)


def test_changes_on_version(monkeypatch):
    fp1 = compute_fingerprint(_StubAsset)
    monkeypatch.setattr("data_assets.__version__", "99.0.0")
    fp2 = compute_fingerprint(_StubAsset)
    assert fp1 != fp2
