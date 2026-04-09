"""Generate systemd unit files for automated package upgrade + DAG sync."""

from __future__ import annotations

from string import Template

SERVICE_TEMPLATE = Template("""\
[Unit]
Description=Upgrade data-assets package and sync Airflow DAG files
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/bin/bash -c '$pip_install && $sync_command'
User=$user
Environment="PATH=$venv_bin:/usr/local/bin:/usr/bin:/bin"
StandardOutput=journal
StandardError=journal
""")

TIMER_TEMPLATE = Template("""\
[Unit]
Description=Periodic data-assets DAG sync

[Timer]
OnBootSec=5min
OnUnitActiveSec=${interval}min
RandomizedDelaySec=60
Persistent=true

[Install]
WantedBy=timers.target
""")

SETUP_TEMPLATE = Template("""\
#!/bin/bash
# One-time setup for data-assets DAG sync.
# Review this script, then run: sudo bash $setup_script_name
set -euo pipefail

AIRFLOW_USER="$user"
DAG_DIR="$dag_dir"

# Ensure DAG output directory exists with correct ownership
mkdir -p "$$DAG_DIR"
chown "$$AIRFLOW_USER":"$$AIRFLOW_USER" "$$DAG_DIR"

# Install systemd unit files
install -m 644 "$service_name" /etc/systemd/system/
install -m 644 "$timer_name" /etc/systemd/system/
restorecon -v /etc/systemd/system/data-assets-sync.* 2>/dev/null || true

# Enable and start the timer
systemctl daemon-reload
systemctl enable --now data-assets-sync.timer

echo ""
echo "Setup complete. Verify with:"
echo "  systemctl status data-assets-sync.timer"
echo "  journalctl -u data-assets-sync.service"
""")


def generate_systemd_units(
    output_dir: str,
    dag_dir: str,
    *,
    venv_path: str = "/opt/airflow/venv",
    pip_index_url: str | None = None,
    interval_minutes: int = 15,
    user: str = "airflow",
) -> tuple[str, str, str]:
    """Return (service_content, timer_content, setup_script) for systemd.

    Args:
        output_dir: Directory where the generated files will be written.
        dag_dir: Airflow DAGs directory (passed to ``data-assets sync --output-dir``).
        venv_path: Path to the Python virtual environment (contains bin/pip, bin/data-assets).
        pip_index_url: Custom PyPI index URL (e.g. corporate Nexus). If None, uses default.
        interval_minutes: How often the timer fires (default 15 min).
        user: System user to run the service as (default "airflow").
    """
    venv_bin = f"{venv_path}/bin"
    pip = f"{venv_bin}/pip"

    index_flag = f" --index-url {pip_index_url}" if pip_index_url else ""
    pip_install = f"{pip} install --upgrade data-assets{index_flag}"
    sync_command = f"{venv_bin}/data-assets sync --output-dir {dag_dir}"

    service_name = "data-assets-sync.service"
    timer_name = "data-assets-sync.timer"
    setup_script_name = "data-assets-setup.sh"

    service = SERVICE_TEMPLATE.substitute(
        pip_install=pip_install,
        sync_command=sync_command,
        user=user,
        venv_bin=venv_bin,
    )

    timer = TIMER_TEMPLATE.substitute(interval=interval_minutes)

    setup = SETUP_TEMPLATE.substitute(
        user=user,
        dag_dir=dag_dir,
        service_name=service_name,
        timer_name=timer_name,
        setup_script_name=setup_script_name,
    )

    return service, timer, setup
