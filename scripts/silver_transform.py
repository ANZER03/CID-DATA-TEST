from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, current_timestamp, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("silver_transactions").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# spark-defaults.conf points warehouse to s3a://bronze/ (for the bronze job).
# Override it here so the Nessie catalog default root is silver for this job.
spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://silver/")

# ── Read bronze ──────────────────────────────────────────────────────────────
bronze_df = spark.table("nessie.bronze.transactions")

# ── Deduplicate: keep latest row per id_transaction ──────────────────────────
window = Window.partitionBy("id_transaction").orderBy(col("timestamp").desc())

silver_df = (
    bronze_df
    .withColumn("row_num", row_number().over(window))
    .filter(col("row_num") == 1)
    .drop("row_num")
    .select(
        col("id_transaction"),
        upper(col("client")).alias("client_name"),
        col("produit").alias("product_name"),
        col("prix").alias("price"),
        col("timestamp").cast("timestamp").alias("transaction_time"),
        current_timestamp().alias("processed_at"),
    )
)

# ── Create silver namespace + table if needed ─────────────────────────────────
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")

spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.transactions (
        id_transaction  STRING,
        client_name     STRING,
        product_name    STRING,
        price           DOUBLE,
        transaction_time TIMESTAMP,
        processed_at    TIMESTAMP
    )
    USING iceberg
    LOCATION 's3a://silver/transactions/'
""")

# ── Full overwrite (mirrors dbt materialized='table') ─────────────────────────
silver_df.writeTo("nessie.silver.transactions").overwritePartitions()

print(f"✅ Silver done — {silver_df.count()} rows written")
spark.stop()
