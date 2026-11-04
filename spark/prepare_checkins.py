"""Clean Gowalla check-ins, add local time and H3 cells, write Parquet.

Run from the repo root:  python spark/prepare_checkins.py
Raw format (tab-separated, no header): user, check-in time (UTC, ISO), latitude, longitude, location id
"""
from pathlib import Path

import h3
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import pandas_udf

RAW_CHECKINS = Path("data/raw/loc-gowalla_totalCheckins.txt")
OUT = Path("data/clean/checkins")
H3_RES = 9

#utc, connect, local
spark = (
    SparkSession.builder.appName("prepare_checkins")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# no header data, assign
raw = spark.read.csv(str(RAW_CHECKINS), sep="\t").toDF("user_id", "ts", "lat", "lon", "venue_id")

#filter based on data explore finding, such as out of range coordinates 
checkins = (
    raw.withColumn("user_id", F.col("user_id").cast("long"))
    .withColumn("venue_id", F.col("venue_id").cast("long"))
    .withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .withColumn("ts_utc", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
    .drop("ts")
    .filter(F.col("lat").between(-90, 90) & F.col("lon").between(-180, 180))
    .filter(~((F.col("lat") == 0) & (F.col("lon") == 0)))
    .filter(F.col("ts_utc").isNotNull())
)

_tf = None


@pandas_udf("string")
def timezone_name(lat: pd.Series, lon: pd.Series) -> pd.Series:
    # Gowalla has no time zone column, so derive it from coordinates.
    global _tf
    if _tf is None:
        from timezonefinder import TimezoneFinder
        _tf = TimezoneFinder()
    return pd.Series([_tf.timezone_at(lng=b, lat=a) or "UTC" for a, b in zip(lat, lon)])


@pandas_udf("long")
def h3_cell(lat: pd.Series, lon: pd.Series) -> pd.Series:
    # Vectorised (pandas) UDF: processes batches, far faster than a row-by-row Python UDF.
    return pd.Series([h3.str_to_int(h3.latlng_to_cell(a, b, H3_RES)) for a, b in zip(lat, lon)])


# Look up time zone and H3 once per venue (far fewer venues than check-ins), then join back.
venues = (
    checkins.groupBy("venue_id")
    .agg(F.first("lat").alias("v_lat"), F.first("lon").alias("v_lon"))
    .withColumn("tz_name", timezone_name("v_lat", "v_lon"))
    .withColumn("h3_r9", h3_cell("v_lat", "v_lon"))
    .select("venue_id", "tz_name", "h3_r9")
)


# step 1 checkins by venue first, only add new coordinates fo the venues that moved
# find the timezone for each venue
df = (
    checkins.join(venues, "venue_id")
    .withColumn("ts_local_ts", F.from_utc_timestamp("ts_utc", F.col("tz_name")))
    .withColumn(
        "tz_offset_min",
        ((F.unix_timestamp("ts_local_ts") - F.unix_timestamp("ts_utc")) / 60).cast("int"),
    )
    .drop("ts_local_ts")
    .withColumn("month", F.date_format("ts_utc", "yyyy-MM"))
)

df.write.mode("overwrite").partitionBy("month").parquet(str(OUT))

#process by partition, overwrite
result = spark.read.parquet(str(OUT))
print(f"Rows written: {result.count():,}")
result.groupBy("month").count().orderBy("month").show(40)
result.groupBy("tz_name").count().orderBy(F.desc("count")).show(10, truncate=False)
