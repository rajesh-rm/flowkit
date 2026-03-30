"""engmx_flowkit — Turnkey data-as-asset ETLs for Apache Airflow 3.1+.

Install the package, configure Airflow Connections for your sources,
and call generate_dags() from a stub DAG file. That's it.

Usage::

    # dags/flowkit_dags.py
    from engmx_flowkit import generate_dags
    globals().update(generate_dags())
"""

__version__ = "0.1.0"
