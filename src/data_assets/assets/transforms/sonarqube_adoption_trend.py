"""Weekly SonarQube adoption trend — new-project onboardings over time."""

from __future__ import annotations

from sqlalchemy import BigInteger, Date

from data_assets.core.column import Column, Index
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.transform_asset import TransformAsset
from data_assets.db.dialect import Dialect


@register
class SonarqubeAdoptionTrend(TransformAsset):
    name = "sonarqube_adoption_trend"
    description = (
        "Weekly count of newly-onboarded SonarQube projects with a running "
        "cumulative total. Week = ISO Monday-start. Onboarding = earliest "
        "analysis_date per project_key in sonarqube_measures_history. "
        "Gap-free through the last completed week for PowerBI time series."
    )
    target_table = "sonarqube_adoption_trend"
    source_tables = ["sonarqube_measures_history"]

    columns = [
        Column("week_start_date",     Date(),       nullable=False),
        Column("new_projects",        BigInteger(), nullable=False),
        Column("cumulative_projects", BigInteger(), nullable=False),
    ]
    primary_key = ["week_start_date"]
    indexes = [Index(columns=("week_start_date",))]

    def query(self, context: RunContext, dialect: Dialect) -> str:
        wk       = dialect.week_start_from_ts
        add_days = dialect.date_add_days

        return f"""
        WITH RECURSIVE
        last_completed AS (
            SELECT {wk(add_days('CURRENT_DATE', -7))} AS wk
        ),
        onboardings AS (
            SELECT
                project_key,
                {wk('MIN(analysis_date)')} AS onboarded_week
            FROM raw.sonarqube_measures_history
            WHERE analysis_date <= NOW()
            GROUP BY project_key
        ),
        weekly_counts AS (
            SELECT
                onboarded_week AS week_start_date,
                COUNT(*) AS new_projects
            FROM onboardings
            WHERE onboarded_week <= (SELECT wk FROM last_completed)
            GROUP BY onboarded_week
        ),
        week_bounds AS (
            SELECT
                MIN(week_start_date) AS first_week,
                GREATEST(
                    MAX(week_start_date),
                    (SELECT wk FROM last_completed)
                ) AS last_week
            FROM weekly_counts
        ),
        spine AS (
            SELECT first_week AS week_start_date
            FROM week_bounds
            WHERE first_week IS NOT NULL
            UNION ALL
            SELECT {add_days('spine.week_start_date', 7)}
            FROM spine, week_bounds
            WHERE spine.week_start_date < week_bounds.last_week
        )
        SELECT
            s.week_start_date,
            COALESCE(c.new_projects, 0) AS new_projects,
            {dialect.cast_bigint(
                'SUM(COALESCE(c.new_projects, 0)) OVER (ORDER BY s.week_start_date)'
            )} AS cumulative_projects
        FROM spine s
        LEFT JOIN weekly_counts c
            ON c.week_start_date = s.week_start_date
        ORDER BY s.week_start_date
        """
