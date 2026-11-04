"""Load cleaned Gowalla checkins and the friendship network into BigQuery"""
import os
import pandas as pd
from google.cloud import bigquery
from pathlib import Path

#project id from google
PROJECT = os.environ["BQ_PROJECT"]
DATASET = "raw"

#connect, create the dataset if it isnt there 
client = bigquery.Client(project = PROJECT)
client.create_dataset(f"{PROJECT}.{DATASET}", exists_ok = True)

#instead of use pandas to read first then big query convert into another copy
# just upload to sparks parquet directly, save memory
files = sorted(Path("data/clean/checkins").glob("month=*/*.parquet"))
print(f"Uploading {len(files)} parquet files")
table_id = f"{PROJECT}.{DATASET}.checkins"

for i,path in enumerate(files):
    config = bigquery.LoadJobConfig(
        source_format = bigquery.SourceFormat.PARQUET,
        write_disposition = "WRITE_TRUNCATE" if i ==0 else "WRITE_APPEND",
        time_partitioning = bigquery.TimePartitioning(field = "ts_utc"),
        clustering_fields = ["user_id"]
    )
    with open(path, "rb") as f:
        client.load_table_from_file(f,table_id, job_config = config ).result()
    print(f"  {i + 1}/{len(files)} {path.parent.name}")
print(f"checkins: {client.get_table(table_id).num_rows:,} rows")
    

# bigquery allows up to 10k paritions, use default day parition, we have about 600 days of data which makes sense tto partition by day
# cluster by user cuz features are by user and filter on date typically

#load friend
friends = pd.read_csv(
    "data/raw/loc-gowalla_edges.txt", sep = "\t", header = None, names = ["user_a", "user_b"]
)
friends_id = f"{PROJECT}.{DATASET}.friendships"
config2 = bigquery.LoadJobConfig(write_disposition = "WRITE_TRUNCATE")
client.load_table_from_dataframe(friends, friends_id, job_config = config2).result()
print(f"friendships: {client.get_table(friends_id).num_rows:,} rows")