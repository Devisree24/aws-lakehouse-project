"""
Glue Batch ETL: Raw CSV (Data Catalog) -> Curated Parquet
- Reads: customers, orders, products from lakehouse_db
- Validates orders (nulls, qty>0, dedup by order_id)
- Writes invalid rows to quarantine/orders/
- Writes quality report to quality_reports/
- Writes Parquet: dim_customers, dim_products, orders (partitioned year/month/day)
"""
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ------------------------------------------------------------------
# CONFIG – CHANGE BUCKET NAME IF DIFFERENT
# ------------------------------------------------------------------
BUCKET = "de-lakehouse-devis-us-east-1"
RAW_DB = "lakehouse_db"

# Glue table names (must match your Data Catalog)
TABLE_CUSTOMERS = "customers"
TABLE_PRODUCTS = "products"
TABLE_ORDERS = "orders"

# Paths
RAW_PREFIX = f"s3://{BUCKET}/raw"
QUARANTINE_ORDERS = f"s3://{BUCKET}/quarantine/orders/"
QUALITY_REPORTS = f"s3://{BUCKET}/quality_reports/"
CURATED_DIM_CUSTOMERS = f"s3://{BUCKET}/curated/dim_customers/"
CURATED_DIM_PRODUCTS = f"s3://{BUCKET}/curated/dim_products/"
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
# READ RAW TABLES FROM GLUE CATALOG
# ------------------------------------------------------------------
def read_table(table_name):
    return glueContext.create_dynamic_frame.from_catalog(
        database=RAW_DB,
        table_name=table_name,
    ).toDF()

raw_customers_df = read_table(TABLE_CUSTOMERS)
raw_products_df = read_table(TABLE_PRODUCTS)
raw_orders_df = read_table(TABLE_ORDERS)

# ------------------------------------------------------------------
# ORDERS: CAST & PARSE
# ------------------------------------------------------------------
raw_orders_df = (
    raw_orders_df
    .withColumn("qty", F.col("qty").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("double"))
)

orders_with_ts = raw_orders_df.withColumn(
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
# DATA QUALITY: VALID vs INVALID
# ------------------------------------------------------------------
valid_condition = (
    F.col("customer_id").isNotNull()
    & F.col("product_id").isNotNull()
    & (F.col("order_ts").isNotNull() & (F.col("order_ts") != ""))
    & (F.col("qty") > 0)
)

valid_orders = orders_with_ts.filter(valid_condition)
invalid_orders = orders_with_ts.filter(~valid_condition)

# Dedup by order_id (keep latest order_ts_parsed)
w = Window.partitionBy("order_id").orderBy(F.col("order_ts_parsed").desc())
valid_orders_dedup = (
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

# All invalid: bad rows + duplicates
invalid_all = invalid_orders.unionByName(
    duplicate_rows, allowMissingColumns=True
)

# ------------------------------------------------------------------
# WRITE QUARANTINE
# ------------------------------------------------------------------
if invalid_all.count() > 0:
    (
        invalid_all.write
        .mode("append")
        .format("parquet")
        .save(QUARANTINE_ORDERS)
    )

# ------------------------------------------------------------------
# QUALITY REPORT
# ------------------------------------------------------------------
total_raw = raw_orders_df.count()
total_valid = valid_orders_dedup.count()
total_invalid = invalid_all.count()
quality_df = spark.createDataFrame(
    [
        (total_raw, total_valid, total_invalid),
    ],
    ["total_raw", "total_valid", "total_invalid"],
).withColumn("run_ts", F.current_timestamp())

(
    quality_df.write
    .mode("append")
    .format("json")
    .save(QUALITY_REPORTS)
)

# ------------------------------------------------------------------
# CURATED: DIMENSIONS
# ------------------------------------------------------------------
dim_customers = raw_customers_df.dropDuplicates(["customer_id"])
dim_customers.write.mode("overwrite").format("parquet").save(
    CURATED_DIM_CUSTOMERS
)

dim_products = raw_products_df.dropDuplicates(["product_id"])
dim_products.write.mode("overwrite").format("parquet").save(
    CURATED_DIM_PRODUCTS
)

# ------------------------------------------------------------------
# CURATED: ORDERS (PARTITIONED)
# ------------------------------------------------------------------
(
    valid_orders_dedup.write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .format("parquet")
    .save(CURATED_ORDERS)
)

job.commit()
