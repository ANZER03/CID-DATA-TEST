"""
silver_olist.py
---------------
Reads raw Olist CSVs from the bronze MinIO bucket, applies cleaning,
type casting, validation, deduplication and derives a small set of
business columns.  Writes Iceberg tables to the silver bucket via the
Nessie catalog.

Run (inside spark container or via Airflow spark-submit):
    spark-submit /opt/airflow/scripts/silver_olist.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType,
)

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Spark session
# ─────────────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("silver_olist")
    .config("spark.sql.catalog.nessie.warehouse", "s3a://silver/")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────────
# 2.  Helpers
# ─────────────────────────────────────────────────────────────────────────────

BRONZE = "s3a://bronze"


def read_csv(filename: str, schema: StructType) -> DataFrame:
    """Read a CSV from the bronze bucket with a strict schema."""
    path = f"{BRONZE}/{filename}"
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(schema)
        .csv(path)
    )


def write_silver(df: DataFrame, table: str, partition_col: str = None,
                 sort_cols: list = None) -> None:
    """Write to a silver Iceberg table (full overwrite each run)."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

    if sort_cols:
        df = df.sortWithinPartitions(*sort_cols)

    full_table = f"nessie.silver.{table}"

    if partition_col:
        # Iceberg partition transforms (months, days, bucket, etc.) only work
        # in SQL DDL, not in the PySpark writeTo().partitionedBy() API.
        # Use CREATE TABLE ... AS SELECT instead.
        tmp_view = f"_tmp_{table}"
        df.createOrReplaceTempView(tmp_view)
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        spark.sql(f"""
            CREATE TABLE {full_table}
            USING iceberg
            PARTITIONED BY ({partition_col})
            AS SELECT * FROM {tmp_view}
        """)
        spark.catalog.dropTempView(tmp_view)
    else:
        # Drop first to clear Nessie metadata (avoids stale S3 references)
        spark.sql(f"DROP TABLE IF EXISTS {full_table}")
        df.writeTo(full_table).using("iceberg").create()

    count = spark.table(full_table).count()
    print(f"✅ silver.{table} — {count:,} rows written")


# ─────────────────────────────────────────────────────────────────────────────
# 3.  Schemas (explicit — never inferred)
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_CUSTOMERS = StructType([
    StructField("customer_id",              StringType()),
    StructField("customer_unique_id",       StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city",            StringType()),
    StructField("customer_state",           StringType()),
])

SCHEMA_ORDERS = StructType([
    StructField("order_id",                      StringType()),
    StructField("customer_id",                   StringType()),
    StructField("order_status",                  StringType()),
    StructField("order_purchase_timestamp",      StringType()),
    StructField("order_approved_at",             StringType()),
    StructField("order_delivered_carrier_date",  StringType()),
    StructField("order_delivered_customer_date", StringType()),
    StructField("order_estimated_delivery_date", StringType()),
])

SCHEMA_ORDER_ITEMS = StructType([
    StructField("order_id",            StringType()),
    StructField("order_item_id",       IntegerType()),
    StructField("product_id",          StringType()),
    StructField("seller_id",           StringType()),
    StructField("shipping_limit_date", StringType()),
    StructField("price",               DoubleType()),
    StructField("freight_value",       DoubleType()),
])

SCHEMA_PRODUCTS = StructType([
    StructField("product_id",                StringType()),
    StructField("product_category_name",     StringType()),
    StructField("product_name_length",       IntegerType()),
    StructField("product_description_length",IntegerType()),
    StructField("product_photos_qty",        IntegerType()),
    StructField("product_weight_g",          DoubleType()),
    StructField("product_length_cm",         DoubleType()),
    StructField("product_height_cm",         DoubleType()),
    StructField("product_width_cm",          DoubleType()),
])

SCHEMA_CATEGORY_TRANSLATION = StructType([
    StructField("product_category_name",         StringType()),
    StructField("product_category_name_english", StringType()),
])

SCHEMA_SELLERS = StructType([
    StructField("seller_id",              StringType()),
    StructField("seller_zip_code_prefix", StringType()),
    StructField("seller_city",            StringType()),
    StructField("seller_state",           StringType()),
])

SCHEMA_PAYMENTS = StructType([
    StructField("order_id",             StringType()),
    StructField("payment_sequential",   IntegerType()),
    StructField("payment_type",         StringType()),
    StructField("payment_installments", IntegerType()),
    StructField("payment_value",        DoubleType()),
])

SCHEMA_REVIEWS = StructType([
    StructField("review_id",               StringType()),
    StructField("order_id",                StringType()),
    StructField("review_score",            IntegerType()),
    StructField("review_comment_title",    StringType()),
    StructField("review_comment_message",  StringType()),
    StructField("review_creation_date",    StringType()),
    StructField("review_answer_timestamp", StringType()),
])

SCHEMA_GEOLOCATION = StructType([
    StructField("geolocation_zip_code_prefix", StringType()),
    StructField("geolocation_lat",             DoubleType()),
    StructField("geolocation_lng",             DoubleType()),
    StructField("geolocation_city",            StringType()),
    StructField("geolocation_state",           StringType()),
])

# ─────────────────────────────────────────────────────────────────────────────
# 4.  Silver transformations
# ─────────────────────────────────────────────────────────────────────────────

def process_customers() -> None:
    raw = read_csv("olist_customers_dataset.csv", SCHEMA_CUSTOMERS)

    df = (
        raw
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("customer_unique_id").isNotNull())
        .withColumn("customer_city",
                    F.initcap(F.trim(F.col("customer_city"))))
        .withColumn("customer_state",
                    F.upper(F.trim(F.col("customer_state"))))
        .withColumn("customer_zip_code_prefix",
                    F.trim(F.col("customer_zip_code_prefix")))
        .dropDuplicates(["customer_id"])
        .withColumn("processed_at", F.current_timestamp())
    )

    write_silver(df, "customers", sort_cols=["customer_state", "customer_id"])


def process_orders() -> None:
    raw = read_csv("olist_orders_dataset.csv", SCHEMA_ORDERS)

    ts_cols = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]

    df = raw
    for c in ts_cols:
        df = df.withColumn(c, F.to_timestamp(F.col(c), "yyyy-MM-dd HH:mm:ss"))

    df = (
        df
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .dropDuplicates(["order_id"])
        # Derived columns
        .withColumn("purchase_date",
                    F.to_date(F.col("order_purchase_timestamp")))
        .withColumn("purchase_year_month",
                    F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM"))
        .withColumn("is_delivered",
                    F.col("order_status") == "delivered")
        .withColumn("is_cancelled",
                    F.col("order_status") == "canceled")
        .withColumn("delivery_duration_days",
                    F.when(
                        F.col("order_delivered_customer_date").isNotNull(),
                        F.datediff(F.col("order_delivered_customer_date"),
                                   F.col("order_purchase_timestamp"))
                    ))
        .withColumn("delivery_delay_days",
                    F.when(
                        F.col("order_delivered_customer_date").isNotNull() &
                        F.col("order_estimated_delivery_date").isNotNull(),
                        F.datediff(F.col("order_delivered_customer_date"),
                                   F.col("order_estimated_delivery_date"))
                    ))
        .withColumn("is_late_delivery",
                    F.when(F.col("delivery_delay_days").isNotNull(),
                           F.col("delivery_delay_days") > 0).otherwise(False))
        .withColumn("processed_at", F.current_timestamp())
    )

    # Repartition by month before writing
    df = df.repartition(F.col("purchase_year_month"))

    write_silver(
        df, "orders",
        partition_col="months(order_purchase_timestamp)",
        sort_cols=["order_purchase_timestamp", "customer_id"],
    )


def process_order_items() -> None:
    raw = read_csv("olist_order_items_dataset.csv", SCHEMA_ORDER_ITEMS)

    df = (
        raw
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("order_item_id").isNotNull())
        .filter(F.col("price").isNotNull() & (F.col("price") >= 0))
        .filter(F.col("freight_value").isNotNull() & (F.col("freight_value") >= 0))
        .withColumn("shipping_limit_date",
                    F.to_timestamp(F.col("shipping_limit_date"), "yyyy-MM-dd HH:mm:ss"))
        .dropDuplicates(["order_id", "order_item_id"])
        .withColumn("total_item_amount",
                    F.col("price") + F.col("freight_value"))
        .withColumn("processed_at", F.current_timestamp())
    )

    write_silver(df, "order_items", sort_cols=["order_id", "order_item_id"])


def process_products() -> None:
    raw_products    = read_csv("olist_products_dataset.csv", SCHEMA_PRODUCTS)
    raw_translation = read_csv("product_category_name_translation.csv",
                               SCHEMA_CATEGORY_TRANSLATION)

    translation = (
        raw_translation
        .withColumn("product_category_name",
                    F.trim(F.lower(F.col("product_category_name"))))
        .withColumn("product_category_name_english",
                    F.trim(F.col("product_category_name_english")))
        .dropDuplicates(["product_category_name"])
    )

    df = (
        raw_products
        .filter(F.col("product_id").isNotNull())
        .withColumn("product_category_name",
                    F.trim(F.lower(F.col("product_category_name"))))
        .dropDuplicates(["product_id"])
        .join(translation, on="product_category_name", how="left")
        .withColumn("product_weight_g",
                    F.when(F.col("product_weight_g") >= 0,
                           F.col("product_weight_g")))
        .withColumn("product_volume_cm3",
                    F.when(
                        F.col("product_length_cm").isNotNull() &
                        F.col("product_height_cm").isNotNull() &
                        F.col("product_width_cm").isNotNull(),
                        F.col("product_length_cm") *
                        F.col("product_height_cm") *
                        F.col("product_width_cm")
                    ))
        .withColumn("has_category",
                    F.col("product_category_name").isNotNull())
        .withColumnRenamed("product_category_name",         "product_category_name_pt")
        .withColumnRenamed("product_category_name_english", "product_category_name_en")
        .withColumn("processed_at", F.current_timestamp())
    )

    write_silver(df, "products", sort_cols=["product_id"])


def process_sellers() -> None:
    raw = read_csv("olist_sellers_dataset.csv", SCHEMA_SELLERS)

    df = (
        raw
        .filter(F.col("seller_id").isNotNull())
        .withColumn("seller_city",
                    F.initcap(F.trim(F.col("seller_city"))))
        .withColumn("seller_state",
                    F.upper(F.trim(F.col("seller_state"))))
        .withColumn("seller_zip_code_prefix",
                    F.trim(F.col("seller_zip_code_prefix")))
        .dropDuplicates(["seller_id"])
        .withColumn("processed_at", F.current_timestamp())
    )

    write_silver(df, "sellers", sort_cols=["seller_state", "seller_id"])


def process_payments() -> None:
    raw = read_csv("olist_order_payments_dataset.csv", SCHEMA_PAYMENTS)

    df = (
        raw
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0))
        .filter(F.col("payment_installments").isNotNull() &
                (F.col("payment_installments") >= 0))
        .withColumn("payment_type",
                    F.lower(F.trim(F.col("payment_type"))))
        .dropDuplicates(["order_id", "payment_sequential"])
        .withColumn("processed_at", F.current_timestamp())
    )

    write_silver(df, "payments", sort_cols=["order_id", "payment_sequential"])


def process_reviews() -> None:
    raw = read_csv("olist_order_reviews_dataset.csv", SCHEMA_REVIEWS)

    df = (
        raw
        .filter(F.col("review_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(
            F.col("review_score").isNotNull() &
            (F.col("review_score") >= 1) &
            (F.col("review_score") <= 5)
        )
        .withColumn("review_creation_date",
                    F.to_timestamp(F.col("review_creation_date"),
                                   "yyyy-MM-dd HH:mm:ss"))
        .withColumn("review_answer_timestamp",
                    F.to_timestamp(F.col("review_answer_timestamp"),
                                   "yyyy-MM-dd HH:mm:ss"))
        .withColumn("review_comment_title",
                    F.when(F.trim(F.col("review_comment_title")) != "",
                           F.trim(F.col("review_comment_title"))))
        .withColumn("review_comment_message",
                    F.when(F.trim(F.col("review_comment_message")) != "",
                           F.trim(F.col("review_comment_message"))))
        .dropDuplicates(["review_id", "order_id"])
        .withColumn("has_review_message",
                    F.col("review_comment_message").isNotNull())
        .withColumn("review_score_category",
                    F.when(F.col("review_score") <= 2, "Negative")
                     .when(F.col("review_score") == 3, "Neutral")
                     .otherwise("Positive"))
        .withColumn("processed_at", F.current_timestamp())
    )

    write_silver(df, "reviews", sort_cols=["order_id"])


def process_geolocation() -> None:
    raw = read_csv("olist_geolocation_dataset.csv", SCHEMA_GEOLOCATION)

    df = (
        raw
        .filter(F.col("geolocation_lat").between(-90,  90))
        .filter(F.col("geolocation_lng").between(-180, 180))
        .withColumn("geolocation_city",
                    F.initcap(F.trim(F.col("geolocation_city"))))
        .withColumn("geolocation_state",
                    F.upper(F.trim(F.col("geolocation_state"))))
    )

    agg = (
        df
        .groupBy(
            "geolocation_zip_code_prefix",
            "geolocation_city",
            "geolocation_state"
        )
        .agg(
            F.avg("geolocation_lat").alias("lat"),
            F.avg("geolocation_lng").alias("lng"),
            F.count("*").alias("coordinate_count"),
        )
        .withColumnRenamed("geolocation_zip_code_prefix", "zip_code_prefix")
        .withColumnRenamed("geolocation_city",            "city")
        .withColumnRenamed("geolocation_state",           "state")
        .withColumn("processed_at", F.current_timestamp())
    )

    write_silver(agg, "geolocation", sort_cols=["state", "zip_code_prefix"])


# ─────────────────────────────────────────────────────────────────────────────
# 5.  Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🚀 Starting Silver job — Olist dataset")

    process_customers()
    process_orders()
    process_order_items()
    process_products()
    process_sellers()
    process_payments()
    process_reviews()
    process_geolocation()

    print("🏁 Silver job complete")
    spark.stop()
