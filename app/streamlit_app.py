"""Live demo. Reads small exported result files from app/data, so it needs no cloud credentials.

Run locally:  streamlit run app/streamlit_app.py
"""
from pathlib import Path

import duckdb
import streamlit as st

DATA = Path(__file__).parent / "data"

st.title("Do friends who meet up stick around?")
st.caption("Foursquare check-ins and friendships. Pipeline: Spark, BigQuery, dbt, Dagster.")

retention_file = DATA / "retention_by_segment.parquet"
if retention_file.exists():
    st.subheader("Retention by friend meetups in the first 30 days")
    st.dataframe(duckdb.sql(f"select * from read_parquet('{retention_file}')").df())
else:
    st.info("Export retention_by_segment.parquet to app/data to show results here.")
