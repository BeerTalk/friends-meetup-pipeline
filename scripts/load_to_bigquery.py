"""Load cleaned check-ins and the friendship files into BigQuery (dataset: raw).

Needs two environment variables:
  BQ_PROJECT                      your Google Cloud project id
  GOOGLE_APPLICATION_CREDENTIALS  path to a service account key file
Run from the repo root:  python scripts/load_to_bigquery.py
"""
import os
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
from google.cloud import bigquery

PROJECT = os.environ["BQ_PROJECT"]
DATASET = os.environ.get("BQ_DATASET_RAW", "raw")
RAW = Path("data/raw")
FRIENDS = {
    "friendships_before": RAW / "dataset_WWW_friendship_old.txt",
    "friendships_after": RAW / "dataset_WWW_friendship_new.txt",
}

client = bigquery.Client(project=PROJECT)
client.create_dataset(f"{PROJECT}.{DATASET}", exists_ok=True)


def load(df: pd.DataFrame, table: str, **config) -> None:
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", **config)
    job = client.load_table_from_dataframe(df, f"{PROJECT}.{DATASET}.{table}", job_config=job_config)
    job.result()
    print(f"{table}: {client.get_table(job.destination).num_rows:,} rows")


checkins = ds.dataset("data/clean/checkins", format="parquet", partitioning="hive").to_table().to_pandas()
checkins["month"] = checkins["month"].astype(str)
load(
    checkins,
    "checkins",
    time_partitioning=bigquery.TimePartitioning(field="ts_utc"),
    clustering_fields=["user_id"],
)

for table, path in FRIENDS.items():
    friends = pd.read_csv(path, sep="\t", header=None, names=["user_a", "user_b"])
    load(friends, table)
