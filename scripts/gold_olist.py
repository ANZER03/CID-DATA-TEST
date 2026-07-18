"""
gold_olist.py
-------------
Reads clean Iceberg tables from the silver namespace and writes three
analytics-ready aggregation tables to the gold namespace.

  gold.agg_daily_sales         — one row per purchase date
  gold.agg_monthly_sales       — one row per year-month
  gold.agg_category_performance — one row per year-month + product category

Run (inside spark container or via Airflow spark-submit):
    spark-submit /opt/airflow/scripts/gold_olist.py
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────────────────
# 1.  Spark session
# ─────────────────────────────────────────────────────────────────────────────

spark = (
    SparkSession.builder
    .appName("gold_olist")
    # Silver tables live here
    .config("spark.sql.catalog.nessie.warehouse", "s3a://silver/")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────────────────────────────────────────
# 2.  Helper
# ─────────────────────────────────────────────────────────────────────────────

def write_gold(df: DataFrame, table: str) -> None:
    """Write to a gold Iceberg table (full overwrite each run, unpartitioned)."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
    full_table = f"nessie.gold.{table}"
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")
    df.writeTo(full_table).using("iceberg").tableProperty("location", f"s3a://gold/gold/{table}").create()
    print(f"✅ gold.{table} — {df.count():,} rows written")

# ─────────────────────────────────────────────────────────────────────────────
# 3.  Read silver tables (only the columns we need)
# ─────────────────────────────────────────────────────────────────────────────

orders = spark.table("nessie.silver.orders").select(
    "order_id", "customer_id",
    "order_purchase_timestamp", "purchase_date", "purchase_year_month",
    "order_status", "is_delivered", "is_cancelled", "is_late_delivery",
    "delivery_duration_days", "delivery_delay_days",
)

order_items = spark.table("nessie.silver.order_items").select(
    "order_id", "order_item_id", "product_id", "seller_id",
    "price", "freight_value", "total_item_amount",
)

products = spark.table("nessie.silver.products").select(
    "product_id", "product_category_name_en",
)

reviews = spark.table("nessie.silver.reviews").select(
    "order_id", "review_score",
)

# ─────────────────────────────────────────────────────────────────────────────
# 4.  Shared enriched base: orders + items + (one review score per order)
# ─────────────────────────────────────────────────────────────────────────────

# One review per order (take the first/min score if multiple exist)
review_per_order = (
    reviews
    .groupBy("order_id")
    .agg(F.min("review_score").alias("review_score"))
)

# orders enriched with items and review score
orders_items = (
    orders
    .join(order_items, on="order_id", how="inner")
    .join(review_per_order, on="order_id", how="left")
)

# ─────────────────────────────────────────────────────────────────────────────
# 5.  gold.agg_daily_sales
# ─────────────────────────────────────────────────────────────────────────────

def build_daily_sales() -> None:
    daily = (
        orders_items
        .groupBy("purchase_date")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum(F.when(F.col("is_delivered"), 1).otherwise(0))
             .alias("delivered_order_count"),
            F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0))
             .alias("cancelled_order_count"),
            F.count("order_item_id").alias("items_sold"),
            F.sum("price").alias("gross_revenue"),
            F.sum("freight_value").alias("freight_revenue"),
            F.sum("total_item_amount").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("seller_id").alias("unique_sellers"),
            F.avg("review_score").alias("avg_review_score"),
        )
        .withColumn(
            "avg_order_value",
            F.col("total_revenue") / F.col("order_count"),
        )
        .orderBy("purchase_date")
        .withColumn("processed_at", F.current_timestamp())
    )

    write_gold(daily, "agg_daily_sales")

# ─────────────────────────────────────────────────────────────────────────────
# 6.  gold.agg_monthly_sales
# ─────────────────────────────────────────────────────────────────────────────

def build_monthly_sales() -> None:
    monthly = (
        orders_items
        .groupBy("purchase_year_month")
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.count("order_item_id").alias("items_sold"),
            F.sum("price").alias("gross_revenue"),
            F.sum("freight_value").alias("freight_revenue"),
            F.sum("total_item_amount").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("seller_id").alias("unique_sellers"),
            F.avg("review_score").alias("avg_review_score"),
            F.avg("delivery_duration_days").alias("avg_delivery_days"),
            # Rates
            (F.sum(F.when(F.col("is_delivered"), 1).otherwise(0)) /
             F.countDistinct("order_id")).alias("delivery_success_rate"),
            (F.sum(F.when(F.col("is_cancelled"), 1).otherwise(0)) /
             F.countDistinct("order_id")).alias("cancellation_rate"),
            (F.sum(F.when(F.col("is_late_delivery"), 1).otherwise(0)) /
             F.countDistinct("order_id")).alias("late_delivery_rate"),
        )
        .withColumn(
            "avg_order_value",
            F.col("total_revenue") / F.col("order_count"),
        )
        # Month-over-month revenue growth using a lag window
        .orderBy("purchase_year_month")
    )

    # Add MoM growth with lag
    window = Window.orderBy("purchase_year_month")
    monthly = monthly.withColumn(
        "prev_month_revenue",
        F.lag("total_revenue", 1).over(window),
    ).withColumn(
        "mom_revenue_growth",
        F.when(
            F.col("prev_month_revenue").isNotNull() &
            (F.col("prev_month_revenue") > 0),
            (F.col("total_revenue") - F.col("prev_month_revenue")) /
            F.col("prev_month_revenue"),
        )
    ).drop("prev_month_revenue")

    monthly = monthly.withColumn("processed_at", F.current_timestamp())

    write_gold(monthly, "agg_monthly_sales")

# ─────────────────────────────────────────────────────────────────────────────
# 7.  gold.agg_category_performance
# ─────────────────────────────────────────────────────────────────────────────

def build_category_performance() -> None:
    # Enrich items with category name
    items_with_category = (
        orders_items
        .join(
            F.broadcast(products),
            on="product_id",
            how="left",
        )
        .withColumn(
            "category",
            F.coalesce(F.col("product_category_name_en"), F.lit("unknown"))
        )
    )

    category_perf = (
        items_with_category
        .groupBy("purchase_year_month", "category")
        .agg(
            F.count("order_item_id").alias("units_sold"),
            F.sum("price").alias("gross_revenue"),
            F.sum("total_item_amount").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("price").alias("avg_item_price"),
            F.avg("freight_value").alias("avg_freight"),
            F.avg("review_score").alias("avg_review_score"),
            (F.sum(F.when(F.col("is_late_delivery"), 1).otherwise(0)) /
             F.countDistinct("order_id")).alias("late_delivery_rate"),
        )
        .orderBy("purchase_year_month", "category")
        .withColumn("processed_at", F.current_timestamp())
    )

    write_gold(category_perf, "agg_category_performance")

# ─────────────────────────────────────────────────────────────────────────────
# 8.  Main
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("🚀 Starting Gold job — Olist dataset")

    build_daily_sales()
    build_monthly_sales()
    build_category_performance()

    print("🏁 Gold job complete")
    spark.stop()
