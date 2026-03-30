"""Example: Turnkey stub DAG — generates all configured asset DAGs.

Place this file in your Airflow DAG folder. Configure Airflow Connections
for your sources, and DAGs will appear automatically in the Airflow UI.

See docs/user-guide.md for setup instructions.
"""

from engmx_flowkit import generate_dags

# Generate all configured DAGs — Airflow discovers them from globals()
globals().update(generate_dags())
