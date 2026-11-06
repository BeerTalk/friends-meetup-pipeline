"""Define Dagster, materialize dbt model here, refresh daily

Run from the repo root:  dagster dev -f orchestration/definitions.py
"""
import subprocess
import sys
from pathlib import Path
 
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSelection,
    AssetSpec,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    multi_asset,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets
 
# folder paths
REPO = Path(__file__).resolve().parent.parent
DBT_DIR = REPO / "dbt_project"
 
RAW_FILES = [
    REPO / "data/raw/loc-gowalla_totalCheckins.txt",
    REPO / "data/raw/loc-gowalla_edges.txt",
]
 
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()
 
 
def run_script(context: AssetExecutionContext, args: list[str]) -> None:
    """Run one of the project's scripts from the repo root; fail the asset if it fails."""
    context.log.info(f"Running script: {' '.join(args)}")
    subprocess.run(args, cwd=REPO, check=True)
 
 
# ingest
 
@asset(group_name="ingest", description="Raw Gowalla check-ins and friendships from Stanford SNAP")
def gowalla_raw_files(context: AssetExecutionContext) -> MaterializeResult:
    # the download only needs to happen once: skip it if the files are already there
    if all(path.exists() for path in RAW_FILES):
        context.log.info("Raw files already exist, skipping download.")
    else:
        run_script(context, ["bash", "scripts/download_data.sh"])
    sizes_mb = {path.name: round(path.stat().st_size / 1e6, 2) for path in RAW_FILES}
    return MaterializeResult(metadata={"sizes_mb": sizes_mb})
 
 
@asset(
    group_name="ingest",
    deps=[gowalla_raw_files],
    description="Cleaned Gowalla check-ins with local time and H3 cells, Parquet partitioned by month (Spark)",
)
def clean_checkins_parquet(context: AssetExecutionContext) -> MaterializeResult:
    run_script(context, [sys.executable, "spark/prepare_checkins.py"])
    files = list((REPO / "data/clean/checkins").glob("month=*/*.parquet"))
    return MaterializeResult(metadata={"parquet_files": len(files)})
 
 
# match "raw","checkins" and "raw","friendships" to the dbt sources of the same name,
# which joins the ingest steps onto the dbt graph
@multi_asset(
    group_name="ingest",
    specs=[
        AssetSpec(
            AssetKey(["raw", "checkins"]),
            deps=[clean_checkins_parquet],
            description="BigQuery raw.checkins: partitioned by day, clustered by user",
        ),
        AssetSpec(
            AssetKey(["raw", "friendships"]),
            deps=[gowalla_raw_files],
            description="BigQuery raw.friendships: Gowalla friendships as loaded; each friendship appears twice, once in each direction",
        ),
    ],
)
def big_query_raw_tables(context: AssetExecutionContext):
    run_script(context, [sys.executable, "scripts/load_to_bigquery.py"])
 
    # record each table's row count, so it shows in the Dagster catalog
    from google.cloud import bigquery
 
    client = bigquery.Client()
    for table in ["checkins", "friendships"]:
        rows = client.get_table(f"{client.project}.raw.{table}").num_rows
        yield MaterializeResult(asset_key=AssetKey(["raw", table]), metadata={"num_rows": rows})
 
 
# dbt
 
@dbt_assets(manifest=dbt_project.manifest_path)
def friends_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
 
 
# jobs and schedule 
 
# everyday refresh: dbt only; the ingest steps are slow and only needed when raw data changes
dbt_refresh = define_asset_job("dbt_refresh", selection=AssetSelection.assets(friends_dbt_assets))
 
# everything from the raw files to the retention table
full_pipeline = define_asset_job("full_pipeline", selection=AssetSelection.all())
 
defs = Definitions(
    assets=[gowalla_raw_files, clean_checkins_parquet, big_query_raw_tables, friends_dbt_assets],
    jobs=[dbt_refresh, full_pipeline],
    # refresh the dbt models daily at 6am
    schedules=[ScheduleDefinition(job=dbt_refresh, cron_schedule="0 6 * * *")],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)