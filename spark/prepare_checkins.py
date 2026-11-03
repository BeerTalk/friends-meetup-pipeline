"""Filter raw Foursquare check-ins to one city, add coordinates and H3 cells, write Parquet.

Run from the repo root:  python spark/prepare_checkins.py
After unzipping, check the file names in data/raw and update the RAW_* paths if they differ.
"""
from pathlib import Path

import h3
import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import pandas_udf

RAW = Path("data/raw")
RAW_CHECKINS = RAW / "dataset_WWW_Checkins_anonymized.txt"
RAW_POIS = RAW / "raw_POIs.txt"
OUT = Path("data/clean/checkins")

# City bounding box: New York City. Swap the numbers for another city.
LAT_MIN, LAT_MAX = 40.48, 40.92
LON_MIN, LON_MAX = -74.27, -73.68
H3_RES = 9

spark = (
    SparkSession.builder.appName("prepare_checkins")
    .config("spark.driver.memory", "6g")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# Raw files are tab-separated with no header.
checkins = spark.read.csv(str(RAW_CHECKINS), sep="\t").toDF(
    "user_id", "venue_id", "utc_time", "tz_offset_min"
)
pois = spark.read.csv(str(RAW_POIS), sep="\t").toDF(
    "venue_id", "lat", "lon", "category", "country"
)

# Filter venues to the city first, so the join only touches what we need.
pois = (
    pois.withColumn("lat", F.col("lat").cast("double"))
    .withColumn("lon", F.col("lon").cast("double"))
    .filter(F.col("lat").between(LAT_MIN, LAT_MAX) & F.col("lon").between(LON_MIN, LON_MAX))
)


@pandas_udf("long")
def h3_cell(lat: pd.Series, lon: pd.Series) -> pd.Series:
    # Vectorised (pandas) UDF: processes batches, far faster than a row-by-row Python UDF.
    return pd.Series(
        [h3.str_to_int(h3.latlng_to_cell(a, b, H3_RES)) for a, b in zip(lat, lon)]
    )


df = (
    checkins.join(F.broadcast(pois), "venue_id")  # city venues are small: broadcast avoids a shuffle
    .withColumn("user_id", F.col("user_id").cast("long"))
    .withColumn("tz_offset_min", F.col("tz_offset_min").cast("int"))
    # utc_time looks like "Tue Apr 03 18:00:06 +0000 2012"; drop the weekday before parsing.
    .withColumn("ts_utc", F.to_timestamp(F.substring("utc_time", 5, 30), "MMM dd HH:mm:ss Z yyyy"))
    .withColumn("h3_r9", h3_cell("lat", "lon"))
    .withColumn("month", F.date_format("ts_utc", "yyyy-MM"))
    .drop("utc_time")
)

df.write.mode("overwrite").partitionBy("month").parquet(str(OUT))

result = spark.read.parquet(str(OUT))
print(f"Rows written: {result.count():,}")
result.groupBy("month").count().orderBy("month").show(40)
