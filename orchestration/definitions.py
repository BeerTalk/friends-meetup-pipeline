"""Dagster definitions: every dbt model becomes an asset, refreshed daily.

Run from the repo root:  dagster dev -f orchestration/definitions.py
"""
from pathlib import Path

from dagster import AssetExecutionContext, Definitions, ScheduleDefinition, define_asset_job
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt_project"
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()


@dbt_assets(manifest=dbt_project.manifest_path)
def friends_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


daily_refresh = define_asset_job("daily_refresh", selection="*")

defs = Definitions(
    assets=[friends_dbt_assets],
    jobs=[daily_refresh],
    schedules=[ScheduleDefinition(job=daily_refresh, cron_schedule="0 6 * * *")],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
