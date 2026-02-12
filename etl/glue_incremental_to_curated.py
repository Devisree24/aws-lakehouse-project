"""
Glue Incremental Job (Option A): Merge raw/orders (CSV) + raw/orders_incremental (JSON)
into curated/orders (Parquet). Same validation and dedup as batch.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
BUCKET = "de-lakehouse-devis-us-east-1"
RAW_DB = "lakehouse_db"
TABLE_ORDERS = "orders"

INCREMENTAL_PREFIX = f"s3://{BUCKET}/raw/orders_incremental/"
QUARANTINE_ORDERS = f"s3://{BUCKET}/quarantine/orders/"
QUALITY_REPORTS = f"s3://{BUCKET}/quality_reports/"
CURATED_ORDERS = f"s3://{BUCKET}/curated/orders/"

# ------------------------------------------------------------------
# JOB INIT
# ------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------
# 1) READ RAW ORDERS (CSV from catalog)
# ------------------------------------------------------------------
raw_orders_df = (
    glueContext.create_dynamic_frame.from_catalog(
        database=RAW_DB,
        table_name=TABLE_ORDERS,
    )
    .toDF()
    .withColumn("_source", F.lit("raw"))
)

# ------------------------------------------------------------------
# 2) READ INCREMENTAL ORDERS (JSON from S3)
# ------------------------------------------------------------------
try:
    incremental_df = (
        spark.read.json(INCREMENTAL_PREFIX)
        .withColumn("_source", F.lit("incremental"))
    )
except Exception:
    incremental_df = spark.createDataFrame(
        [], raw_orders_df.drop("_source").schema
    )

# Align column types for union (order_id as string, qty int, unit_price double)
def align_schema(df):
    return (
        df.withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("long"))
        .withColumn("product_id", F.col("product_id").cast("long"))
        .withColumn("order_ts", F.col("order_ts").cast("string"))
        .withColumn("qty", F.col("qty").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn("status", F.col("status").cast("string"))
    )

raw_aligned = align_schema(raw_orders_df).drop("_source")
inc_aligned = align_schema(incremental_df).drop("_source")

# Union: only common columns
common_cols = [c for c in raw_aligned.columns if c in inc_aligned.columns]
raw_union = raw_aligned.select(common_cols)
inc_union = inc_aligned.select(common_cols)
combined = raw_union.unionByName(inc_union, allowMissingColumns=True)

# ------------------------------------------------------------------
# 3) PARSE AND ENRICH (same as batch)
# ------------------------------------------------------------------
orders_with_ts = combined.withColumn(
    "order_ts_parsed",
    F.to_timestamp(F.col("order_ts"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
)
orders_with_ts = (
    orders_with_ts
    .withColumn("order_date", F.to_date("order_ts_parsed"))
    .withColumn("year", F.year("order_ts_parsed"))
    .withColumn("month", F.month("order_ts_parsed"))
    .withColumn("day", F.dayofmonth("order_ts_parsed"))
    .withColumn("revenue", F.col("qty") * F.col("unit_price"))
)

# ------------------------------------------------------------------
# 4) VALIDATION AND DEDUP
# ------------------------------------------------------------------
valid_condition = (
    F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
    & (F.col("order_ts").isNotNull() & (F.col("order_ts") != ""))
    & (F.col("qty") > 0)
)
valid_orders = orders_with_ts.filter(valid_condition)
invalid_orders = orders_with_ts.filter(~valid_condition)

w = Window.partitionBy("order_id").orderBy(F.col("order_ts_parsed").desc())
valid_dedup = (
    valid_orders
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)
duplicate_rows = (
    valid_orders
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") > 1)
    .drop("_rn")
)
invalid_all = invalid_orders.unionByName(duplicate_rows, allowMissingColumns=True)

# ------------------------------------------------------------------
# 5) WRITE QUARANTINE (append)
# ------------------------------------------------------------------
if invalid_all.count() > 0:
    invalid_all.write.mode("append").format("parquet").save(QUARANTINE_ORDERS)

# ------------------------------------------------------------------
# 6) QUALITY REPORT (append)
# ------------------------------------------------------------------
quality_df = spark.createDataFrame(
    [
        (
            combined.count(),
            valid_dedup.count(),
            invalid_all.count(),
        )
    ],
    ["total_raw", "total_valid", "total_invalid"],
).withColumn("run_ts", F.current_timestamp())
quality_df.write.mode("append").format("json").save(QUALITY_REPORTS)

# ------------------------------------------------------------------
# 7) WRITE CURATED ORDERS (overwrite, partitioned)
# ------------------------------------------------------------------
valid_dedup.write.mode("overwrite").partitionBy("year", "month", "day").format(
    "parquet"
).save(CURATED_ORDERS)

job.commit()
