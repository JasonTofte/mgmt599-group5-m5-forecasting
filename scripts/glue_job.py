"""
glue_job.py — M5 wide-to-long transform, AWS Glue (Spark) version.

MGMT 599 Group 5. This is transform_local.py ported to PySpark. Do NOT edit this
file until the local prototype is proven — Glue cold-starts take 2-3 minutes per
run, so debugging here costs ~5 minutes per typo.

GLUE JOB SETUP (console -> ETL jobs -> Script editor -> Spark):
  Type            : Spark
  Glue version    : 4.0 or 5.0
  Language        : Python 3
  Worker type     : G.1X
  Workers         : 3            <- do not raise this, we don't need it
  Job timeout     : 30 minutes
  Job bookmark    : Disable      <- we want full reruns while developing

  Job parameters (Advanced properties -> Job parameters):
    --S3_BUCKET      mgmt599-group5-m5
    --SALES_FILE     sales_train_evaluation.csv

  IAM role needs: s3:GetObject on raw/*, s3:PutObject+DeleteObject on processed/*,
  plus the AWSGlueServiceRole managed policy. Most first-run failures are this.

EXPECTED OUTPUT
  59,181,090 rows total (30,490 series x 1,941 days)
  CA 23,672,436 | TX 17,754,327 | WI 17,754,327
  Written to s3://<bucket>/processed/sales_long/ as Snappy Parquet,
  partitioned by state_id.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# ----------------------------------------------------------------------------
# Setup
# ----------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_BUCKET", "SALES_FILE"])
BUCKET = args["S3_BUCKET"]
SALES_FILE = args["SALES_FILE"]

RAW = f"s3://{BUCKET}/raw"
PROCESSED = f"s3://{BUCKET}/processed/sales_long"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Parquet output tuning: fewer, larger files beat many small ones for Athena
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def log(msg):
    print(f"=== {msg}", flush=True)


# ----------------------------------------------------------------------------
# 1. Read the three source files
# ----------------------------------------------------------------------------
log("reading source CSVs")

sales = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW}/{SALES_FILE}")
)

calendar = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW}/calendar.csv")
)

prices = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW}/sell_prices.csv")
)

day_cols = [c for c in sales.columns if c.startswith("d_")]
id_cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]

n_series = sales.count()
log(f"sales: {n_series:,} series x {len(day_cols):,} day columns")
log(f"expected output rows: {n_series * len(day_cols):,}")

# ----------------------------------------------------------------------------
# 2. Melt wide -> long
# ----------------------------------------------------------------------------
# Build an array of (d, units_sold) structs, then explode it. This is more
# robust than stack() with a 1,941-item expression string, which Spark's parser
# handles but slowly.
log("melting wide to long")

pairs = F.array(*[
    F.struct(F.lit(c).alias("d"), F.col(c).cast(IntegerType()).alias("units_sold"))
    for c in day_cols
])

long_df = (
    sales
    .select(*id_cols, F.explode(pairs).alias("kv"))
    .select(*id_cols, F.col("kv.d").alias("d"), F.col("kv.units_sold").alias("units_sold"))
)

# ----------------------------------------------------------------------------
# 3. Join calendar
# ----------------------------------------------------------------------------
log("joining calendar")

cal_slim = calendar.select(
    "d", "date", "wm_yr_wk", "weekday", "wday", "month", "year",
    "event_name_1", "event_type_1", "event_name_2", "event_type_2",
    "snap_CA", "snap_TX", "snap_WI",
)

# calendar is ~1,969 rows -> broadcast it, avoids a full shuffle on 59M rows
long_df = long_df.join(F.broadcast(cal_slim), on="d", how="left")

# Collapse the three SNAP columns into one that matches the row's own state.
long_df = (
    long_df
    .withColumn(
        "snap_flag",
        F.when(F.col("state_id") == "CA", F.col("snap_CA"))
         .when(F.col("state_id") == "TX", F.col("snap_TX"))
         .when(F.col("state_id") == "WI", F.col("snap_WI"))
         .otherwise(F.lit(0))
         .cast(IntegerType()),
    )
    .drop("snap_CA", "snap_TX", "snap_WI")
)

# ----------------------------------------------------------------------------
# 4. Join prices
# ----------------------------------------------------------------------------
# LEFT join on purpose: an item has no price row for weeks before the store
# carried it. Those nulls are real signal, not missing data to be filled.
log("joining sell_prices")

long_df = long_df.join(
    prices.select("store_id", "item_id", "wm_yr_wk", "sell_price"),
    on=["store_id", "item_id", "wm_yr_wk"],
    how="left",
)

# ----------------------------------------------------------------------------
# 5. Derived columns
# ----------------------------------------------------------------------------
log("adding derived columns")

long_df = (
    long_df
    .withColumn("day_num", F.regexp_replace("d", "d_", "").cast(IntegerType()))
    .withColumn("has_event", F.col("event_name_1").isNotNull().cast(IntegerType()))
    .withColumn("date", F.to_date("date"))
    .withColumn("revenue", F.col("units_sold") * F.coalesce(F.col("sell_price"), F.lit(0.0)))
)

# Column order: partition key (state_id) must come LAST for the writer
long_df = long_df.select(
    "id", "item_id", "dept_id", "cat_id", "store_id",
    "d", "day_num", "date", "wm_yr_wk", "weekday", "wday", "month", "year",
    "units_sold", "sell_price", "revenue",
    "event_name_1", "event_type_1", "event_name_2", "event_type_2",
    "snap_flag", "has_event",
    "state_id",
)

# ----------------------------------------------------------------------------
# 6. Validate BEFORE writing
# ----------------------------------------------------------------------------
log("validating")

actual_rows = long_df.count()
expected_rows = n_series * len(day_cols)
log(f"row count: {actual_rows:,} (expected {expected_rows:,})")

if actual_rows != expected_rows:
    raise RuntimeError(
        f"ROW COUNT MISMATCH: got {actual_rows:,}, expected {expected_rows:,}. "
        "Most likely cause: duplicate (store_id, item_id, wm_yr_wk) keys in "
        "sell_prices inflating the join. Do not write this output."
    )

log("rows by state:")
long_df.groupBy("state_id").count().orderBy("state_id").show()

log("null / quality summary:")
long_df.select(
    F.sum(F.col("units_sold").isNull().cast("int")).alias("null_units"),
    F.sum(F.col("date").isNull().cast("int")).alias("null_date_calendar_join"),
    F.sum(F.col("sell_price").isNull().cast("int")).alias("null_price_expected"),
    F.sum((F.col("units_sold") == 0).cast("int")).alias("zero_sales_rows"),
).show(truncate=False)

# ----------------------------------------------------------------------------
# 7. Write
# ----------------------------------------------------------------------------
log(f"writing to {PROCESSED}")

(
    long_df
    .repartition("state_id")     # one output file set per partition
    .write
    .mode("overwrite")
    .partitionBy("state_id")
    .parquet(PROCESSED)
)

log("done — now run the Glue crawler against processed/sales_long/")
job.commit()
