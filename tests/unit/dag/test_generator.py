"""Tests for data_assets.dag.generator."""

from __future__ import annotations

from data_assets.core.asset import Asset
from data_assets.core.column import Column, Index
from data_assets.core.registry import _registry, register
from data_assets.dag.generator import sync
from data_assets.dag.templates import MANAGED_MARKER

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_stub(name: str, **kwargs):
    """Create and register a minimal stub asset class."""
    attrs = {
        "name": name,
        "target_table": name,
        "columns": [Column("id", "text")],
        "primary_key": ["id"],
        "indexes": [Index(columns=["id"])],
        **kwargs,
    }
    cls = type(name, (Asset,), attrs)
    register(cls)
    return cls


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_sync_creates_files(tmp_path):
    _make_stub("gen_a")
    result = sync(tmp_path)

    assert "dag_gen_a.py" in result.created
    assert (tmp_path / "dag_gen_a.py").exists()


def test_generated_file_is_valid_python(tmp_path):
    _make_stub("gen_valid")
    sync(tmp_path)

    source = (tmp_path / "dag_gen_valid.py").read_text()
    compile(source, "dag_gen_valid.py", "exec")


def test_generated_file_has_marker(tmp_path):
    _make_stub("gen_marker")
    sync(tmp_path)

    first_line = (tmp_path / "dag_gen_marker.py").read_text().split("\n")[0]
    assert first_line.strip() == MANAGED_MARKER


def test_generated_file_has_fingerprint(tmp_path):
    _make_stub("gen_fp")
    sync(tmp_path)

    content = (tmp_path / "dag_gen_fp.py").read_text()
    assert "_ASSET_FINGERPRINT" in content


def test_deterministic(tmp_path):
    _make_stub("gen_det")

    sync(tmp_path)
    first = (tmp_path / "dag_gen_det.py").read_text()

    sync(tmp_path)
    second = (tmp_path / "dag_gen_det.py").read_text()

    assert first == second


def test_skips_unchanged(tmp_path):
    _make_stub("gen_skip")
    sync(tmp_path)
    result = sync(tmp_path)

    assert "dag_gen_skip.py" in result.skipped


def test_orphan_disabled(tmp_path):
    _make_stub("gen_orphan")
    sync(tmp_path)
    assert (tmp_path / "dag_gen_orphan.py").exists()

    # Remove the asset from registry, re-sync
    del _registry["gen_orphan"]
    result = sync(tmp_path)

    assert "dag_gen_orphan.py" in result.disabled
    content = (tmp_path / "dag_gen_orphan.py").read_text()
    assert "DISABLED" in content
    assert "schedule=None" in content


def test_z_prefix_untouched(tmp_path):
    _make_stub("gen_z")
    # Create a z_ file that looks managed
    z_file = tmp_path / "z_custom.py"
    z_file.write_text(f"{MANAGED_MARKER}\n# custom admin DAG\n")
    original = z_file.read_text()

    sync(tmp_path)

    # z_ file should be untouched
    assert z_file.read_text() == original


def test_overrides_toml_untouched(tmp_path):
    _make_stub("gen_toml")
    toml_file = tmp_path / "dag_overrides.toml"
    toml_file.write_text('[gen_toml]\nschedule = "@daily"\n')
    original = toml_file.read_text()

    sync(tmp_path)

    assert toml_file.read_text() == original


def test_overrides_applied(tmp_path):
    _make_stub("gen_ov")
    toml_file = tmp_path / "dag_overrides.toml"
    toml_file.write_text('[gen_ov]\nschedule = "0 3 * * *"\n')

    sync(tmp_path)

    content = (tmp_path / "dag_gen_ov.py").read_text()
    assert '"0 3 * * *"' in content


def test_multi_org_creates_per_org_files(tmp_path):
    _make_stub("gen_mo", source_name="github")
    toml_file = tmp_path / "dag_overrides.toml"
    toml_file.write_text(
        '[gen_mo]\n'
        'secrets_source = "airflow_connection"\n'
        '[[gen_mo.orgs]]\n'
        'org = "org-one"\n'
        'installation_id = "111"\n'
        '[[gen_mo.orgs]]\n'
        'org = "org-two"\n'
        'installation_id = "222"\n'
    )

    result = sync(tmp_path)

    assert "dag_gen_mo_org_one.py" in result.created
    assert "dag_gen_mo_org_two.py" in result.created
    # Each file should have the correct org
    content1 = (tmp_path / "dag_gen_mo_org_one.py").read_text()
    assert 'partition_key="org-one"' in content1
    content2 = (tmp_path / "dag_gen_mo_org_two.py").read_text()
    assert 'partition_key="org-two"' in content2


def test_connection_mode_template(tmp_path):
    _make_stub("gen_conn", source_name="jira")
    toml_file = tmp_path / "dag_overrides.toml"
    toml_file.write_text('[gen_conn]\nsecrets_source = "airflow_connection"\n')

    sync(tmp_path)

    content = (tmp_path / "dag_gen_conn.py").read_text()
    assert "BaseHook.get_connection" in content
    assert "JIRA_EMAIL" in content


def test_description_with_quotes(tmp_path):
    """Descriptions containing quotes produce valid Python."""
    _make_stub("gen_dq", description='He said "hello"')
    sync(tmp_path)

    source = (tmp_path / "dag_gen_dq.py").read_text()
    compile(source, "dag_gen_dq.py", "exec")
    assert "He said" in source


def test_description_with_dollar(tmp_path):
    """Descriptions containing $ don't crash Template.substitute()."""
    _make_stub("gen_dd", description="costs $100")
    sync(tmp_path)

    source = (tmp_path / "dag_gen_dd.py").read_text()
    compile(source, "dag_gen_dd.py", "exec")
    assert "costs $100" in source


def test_non_managed_files_untouched(tmp_path):
    """Files without the managed marker are never modified or disabled."""
    _make_stub("gen_nm")
    custom = tmp_path / "dag_something_else.py"
    custom.write_text("# my hand-written DAG\n")

    sync(tmp_path)

    assert custom.read_text() == "# my hand-written DAG\n"
