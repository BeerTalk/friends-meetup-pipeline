# Do friends who meet up stick around?

Location data pipeline on Foursquare check-ins and friendships (LBSN2Vec++ dataset, CC BY 4.0).
Question: does meeting friends in person early on relate to retention, and could a "friends nearby"
nudge help?

## Stack

Spark (filter and enrich raw data) -> BigQuery (warehouse) -> dbt (models and tests) -> Dagster
(orchestration) -> Streamlit (results).

## Project layout

- `spark/` raw data to clean Parquet with H3 cells
- `scripts/` download and load to BigQuery
- `dbt_project/` staging and mart models, tests
- `orchestration/` Dagster definitions
- `app/` Streamlit demo

## How to run

1. `bash scripts/download_data.sh`
2. `python spark/prepare_checkins.py`
3. Set `BQ_PROJECT` and `GOOGLE_APPLICATION_CREDENTIALS`, then `python scripts/load_to_bigquery.py`
4. `cd dbt_project && dbt build`
5. `dagster dev -f orchestration/definitions.py`

## Findings

To do.

## Data issues found

To do.

## What changes at 100x

To do.
